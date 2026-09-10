package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

const maxProfileBytes = 64 * 1024

var profileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$`)

func profileName(name string) (string, error) {
	if name == "" {
		return "default", nil
	}
	if !profileNamePattern.MatchString(name) {
		return "", errors.New("profile name must be 1-32 ASCII letters, digits, underscores or hyphens, starting with a letter or digit")
	}
	return strings.ToLower(name), nil
}

func basePaths(p Paths) Paths {
	if p.Profile == "" || p.Profile == "default" {
		p.Profile = ""
		return p
	}
	p.Env = filepath.Join(filepath.Dir(p.Example), ".env")
	p.Data = filepath.Dir(filepath.Dir(p.Data))
	p.Profile = ""
	return dataPaths(p)
}

func dataPaths(p Paths) Paths {
	p.DB = filepath.Join(p.Data, "messages.db")
	p.Audio = filepath.Join(p.Data, "audio")
	p.Log = filepath.Join(p.Data, "zello.log")
	p.Socket = filepath.Join(p.Data, "zello.sock")
	p.Lock = filepath.Join(p.Data, "service.lock")
	return p
}

// SelectProfile changes only path selection; it does not create or load anything.
// The default preserves the original .env and data directory.
func SelectProfile(base Paths, name string) (Paths, error) {
	name, err := profileName(name)
	if err != nil {
		return Paths{}, err
	}
	base = basePaths(base)
	if name == "default" {
		return base, nil
	}
	p := base
	p.Profile = name
	p.Env = filepath.Join(filepath.Dir(base.Example), "profiles", name+".json")
	p.Data = filepath.Join(base.Data, "profiles", name)
	p = dataPaths(p)
	if len(p.Socket) > 103 {
		return Paths{}, errors.New("profile data path is too long for a macOS Unix socket (maximum 103 bytes)")
	}
	return p, nil
}

// DecodeProfile accepts one bounded JSON object with the documented uppercase
// keys only. Errors never include a key or value copied from the input.
func DecodeProfile(r io.Reader) (Config, error) {
	invalid := errors.New("invalid profile JSON: use one object containing only documented setting names and string values")
	data, err := io.ReadAll(io.LimitReader(r, maxProfileBytes+1))
	if err != nil || len(data) > maxProfileBytes || !utf8.Valid(data) {
		return Config{}, errors.New("cannot read profile JSON (maximum 64 KiB)")
	}
	var c Config
	fields := map[string]*string{
		"ZELLO_NETWORK": &c.Network, "ZELLO_USERNAME": &c.Username,
		"ZELLO_PASSWORD": &c.Password, "ZELLO_CHANNEL": &c.Channel,
		"OPENAI_API_KEY": &c.OpenAIKey, "OPENAI_TRANSCRIBE_MODEL": &c.TranscribeModel,
		"ELEVENLABS_API_KEY": &c.ElevenLabsKey, "ELEVENLABS_VOICE_ID": &c.VoiceID,
		"ELEVENLABS_MODEL_ID": &c.SpeechModel,
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	first, err := dec.Token()
	if err != nil || first != json.Delim('{') {
		return Config{}, invalid
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return Config{}, invalid
		}
		name, ok := key.(string)
		if !ok || fields[name] == nil {
			return Config{}, invalid
		}
		value, err := dec.Token()
		text, ok := value.(string)
		if err != nil || !ok {
			return Config{}, invalid
		}
		*fields[name] = text
		delete(fields, name) // A repeated decoded key is also an error.
	}
	last, err := dec.Token()
	if err != nil || last != json.Delim('}') {
		return Config{}, invalid
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Config{}, invalid
	}
	return profileDefaults(c)
}

func profileDefaults(c Config) (Config, error) {
	if c.Network == "" {
		c.Network = "sadasant"
	}
	if c.TranscribeModel == "" {
		c.TranscribeModel = "gpt-4o-mini-transcribe"
	}
	if c.SpeechModel == "" {
		c.SpeechModel = "eleven_flash_v2_5"
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]*$`).MatchString(c.Network) {
		return Config{}, errors.New("ZELLO_NETWORK must be a network name")
	}
	return c, nil
}

// privateDir checks only app-owned directories, not system aliases such as /tmp.
func privateDir(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		if err = os.MkdirAll(path, 0700); err == nil {
			info, err = os.Lstat(path)
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	if err != nil {
		return errors.New("cannot access profile directory")
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("profile directories must not be symlinks or other files")
	}
	if create {
		if err := os.Chmod(path, 0700); err != nil {
			return errors.New("cannot make profile directory private")
		}
	} else if info.Mode().Perm() != 0700 {
		return errors.New("profile directories must have mode 0700")
	}
	return nil
}

func profileDirs(base Paths, create bool) error {
	root := filepath.Dir(base.Example)
	for _, dir := range []string{root, filepath.Join(root, "profiles")} {
		if err := privateDir(dir, create); err != nil {
			return err
		}
	}
	return nil
}

// SaveProfile registers credentials once. Hard-link publication prevents a
// concurrent registrar from replacing an existing profile, including a symlink.
func SaveProfile(base Paths, name string, c Config) error {
	p, err := SelectProfile(base, name)
	if err != nil {
		return err
	}
	if p.Profile == "" {
		return errors.New("default is reserved for the existing .env configuration")
	}
	c, err = profileDefaults(c)
	if err != nil {
		return err
	}
	c.Source = p.Env
	if err = c.ValidateService(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil || len(data)+1 > maxProfileBytes {
		return errors.New("profile JSON exceeds the 64 KiB limit")
	}
	if err := profileDirs(base, true); err != nil {
		return err
	}
	dir := filepath.Dir(p.Env)
	f, err := os.CreateTemp(dir, ".profile-*")
	if err != nil {
		return errors.New("cannot create private profile file")
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(append(data, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("cannot write private profile file")
	}
	if err := os.Link(f.Name(), p.Env); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("profile already exists; choose a new name")
		}
		return errors.New("cannot register profile")
	}
	d, err := os.Open(dir)
	if err != nil {
		return errors.New("profile created, but directory durability could not be confirmed")
	}
	err = d.Sync()
	closeErr = d.Close()
	if err != nil || closeErr != nil {
		return errors.New("profile created, but directory durability could not be confirmed")
	}
	return nil
}

func loadProfile(p Paths) (Config, error) {
	if _, err := SelectProfile(basePaths(p), p.Profile); err != nil {
		return Config{}, err
	}
	if err := profileDirs(p, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{}, errors.New("named profile does not exist; register it first")
		}
		return Config{}, err
	}
	info, err := os.Lstat(p.Env)
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, errors.New("named profile does not exist; register it first")
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return Config{}, errors.New("profile credentials must be a regular file with mode 0600, never a symlink")
	}
	fd, err := syscall.Open(p.Env, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return Config{}, errors.New("cannot open profile credentials")
	}
	f := os.NewFile(uintptr(fd), "profile")
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return Config{}, errors.New("profile credentials must be a regular file with mode 0600")
	}
	c, err := DecodeProfile(f)
	if err != nil {
		return Config{}, err
	}
	c.Source = p.Env
	if err := c.ValidateService(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ListProfiles returns names without opening any credential file.
func ListProfiles(base Paths) ([]string, error) {
	names := []string{"default"}
	if err := profileDirs(base, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return names, nil
		}
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(filepath.Dir(base.Example), "profiles"))
	if err != nil {
		return nil, errors.New("cannot list profiles")
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".json")
		canonical, err := profileName(name)
		if err == nil && canonical == name && name != "default" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func prepareProfile(p Paths) error {
	base := basePaths(p)
	if _, err := SelectProfile(base, p.Profile); err != nil {
		return err
	}
	for _, dir := range []string{filepath.Dir(p.Example), base.Data, filepath.Join(base.Data, "profiles"), p.Data, p.Audio} {
		if err := privateDir(dir, true); err != nil {
			return err
		}
	}
	for _, path := range []string{p.DB, p.DB + "-wal", p.DB + "-shm", p.DB + "-journal", p.Lock, p.Log} {
		if err := runtimeFile(path, false); err != nil {
			return err
		}
	}
	if err := runtimeFile(p.Socket, true); err != nil {
		return err
	}
	f, err := os.OpenFile(p.Example, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, err = f.WriteString(Example)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			return errors.New("cannot write configuration example")
		}
	} else if !errors.Is(err, os.ErrExist) {
		return errors.New("cannot create configuration example")
	}
	fd, err := syscall.Open(p.Log, syscall.O_WRONLY|syscall.O_APPEND|syscall.O_CREAT|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errors.New("cannot open profile log")
	}
	f = os.NewFile(uintptr(fd), "profile log")
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return errors.New("cannot make profile log private")
	}
	return f.Close()
}

func runtimeFile(path string, socket bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("cannot inspect profile runtime file")
	}
	if socket {
		if info.Mode()&os.ModeType != os.ModeSocket {
			return errors.New("profile socket must be a socket, never a symlink or another file")
		}
	} else if !info.Mode().IsRegular() {
		return errors.New("profile runtime files must be regular files, never symlinks")
	}
	return nil
}
