// Package config loads the one user-owned configuration file on every invocation.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/joho/godotenv"
)

const Example = `ZELLO_NETWORK=sadasant
ZELLO_USERNAME=
ZELLO_PASSWORD=
ZELLO_CHANNEL=

OPENAI_API_KEY=
OPENAI_TRANSCRIBE_MODEL=gpt-4o-mini-transcribe

ELEVENLABS_API_KEY=
ELEVENLABS_VOICE_ID=
ELEVENLABS_MODEL_ID=eleven_flash_v2_5
`

type Paths struct{ Env, Example, Data, DB, Audio, Log, Socket, Lock string }
type Config struct {
	Network, Username, Password, Channel string
	OpenAIKey, TranscribeModel           string
	ElevenLabsKey, VoiceID, SpeechModel  string
}

func UserPaths() (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, err
	}
	return PathsAt(home), nil
}
func PathsAt(home string) Paths {
	dir := filepath.Join(home, ".local", "share", "zello")
	cfg := filepath.Join(home, ".config", "zello")
	return Paths{filepath.Join(cfg, ".env"), filepath.Join(cfg, ".env.example"), dir, filepath.Join(dir, "messages.db"), filepath.Join(dir, "audio"), filepath.Join(dir, "zello.log"), filepath.Join(dir, "zello.sock"), filepath.Join(dir, "service.lock")}
}
func Prepare(p Paths) error {
	for _, dir := range []string{filepath.Dir(p.Env), p.Data, p.Audio} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(p.Example, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, err = f.WriteString(Example)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	f, err = os.OpenFile(p.Log, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	return f.Close()
}
func Load(p Paths) (Config, error) {
	values, err := godotenv.Read(p.Env)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("cannot parse %s (check .env syntax)", p.Env)
	}
	value := func(k, fallback string) string {
		if v := values[k]; v != "" {
			return v
		}
		return fallback
	}
	c := Config{Network: value("ZELLO_NETWORK", "sadasant"), Username: values["ZELLO_USERNAME"], Password: values["ZELLO_PASSWORD"], Channel: values["ZELLO_CHANNEL"], OpenAIKey: values["OPENAI_API_KEY"], TranscribeModel: value("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe"), ElevenLabsKey: values["ELEVENLABS_API_KEY"], VoiceID: values["ELEVENLABS_VOICE_ID"], SpeechModel: value("ELEVENLABS_MODEL_ID", "eleven_flash_v2_5")}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]*$`).MatchString(c.Network) {
		return Config{}, errors.New("ZELLO_NETWORK must be a network name")
	}
	return c, nil
}
func (c Config) ValidateService() error {
	var missing []string
	if c.Username == "" {
		missing = append(missing, "ZELLO_USERNAME")
	}
	if c.Password == "" {
		missing = append(missing, "ZELLO_PASSWORD")
	}
	if c.Channel == "" {
		missing = append(missing, "ZELLO_CHANNEL")
	}
	if len(missing) > 0 {
		return fmt.Errorf("configure %s in ~/.config/zello/.env", strings.Join(missing, ", "))
	}
	return nil
}
func (c Config) Endpoint() string { return "wss://zellowork.io/ws/" + c.Network }
