package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func profileBase(t *testing.T) Paths {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "zp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(home) })
	return PathsAt(home)
}

func testCredentials() Config {
	return Config{Username: "test-user", Password: "private-marker-password", Channel: "channel"}
}

func selected(t *testing.T, base Paths, name string) Paths {
	t.Helper()
	p, err := SelectProfile(base, name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSelectProfileIsCanonicalAndKeepsIndependentPaths(t *testing.T) {
	base := profileBase(t)
	for _, name := range []string{"", "default", "DEFAULT"} {
		p := selected(t, base, name)
		if !reflect.DeepEqual(p, base) {
			t.Fatal("default changed the existing paths")
		}
	}
	p := selected(t, base, "Work_2-A")
	if p.Profile != "work_2-a" || p.Env != filepath.Join(filepath.Dir(base.Env), "profiles/work_2-a.json") || p.Example != base.Example {
		t.Fatalf("incorrect profile selection: %+v", p)
	}
	if p.Data != filepath.Join(base.Data, "profiles/work_2-a") || p.DB != filepath.Join(p.Data, "messages.db") || p.Audio != filepath.Join(p.Data, "audio") || p.Socket != filepath.Join(p.Data, "zello.sock") || p.Lock != filepath.Join(p.Data, "service.lock") || p.Log != filepath.Join(p.Data, "zello.log") {
		t.Fatal("a profile shares runtime state with the default")
	}
	if again := selected(t, p, "default"); !reflect.DeepEqual(again, base) {
		t.Fatal("reselecting default did not restore the base paths")
	}
	if again := selected(t, p, "Another"); !reflect.DeepEqual(again, selected(t, base, "another")) {
		t.Fatal("selection nested profile paths")
	}
	for _, name := range []string{"../escape", "/absolute", "a/b", "a\\b", ".hidden", "-prefix", " space", "caf\u00e9", strings.Repeat("a", 33)} {
		if _, err := SelectProfile(base, name); err == nil {
			t.Fatalf("accepted unsafe name %q", name)
		}
	}
	if _, err := os.Stat(base.Data); !os.IsNotExist(err) {
		t.Fatal("selection created runtime state")
	}
}

func TestProfileSocketLengthRejectedBeforeCreatingState(t *testing.T) {
	base := profileBase(t)
	base.Data = filepath.Join(base.Data, strings.Repeat("x", 100))
	if _, err := SelectProfile(base, "work"); err == nil {
		t.Fatal("accepted an unusable macOS socket path")
	}
	if err := SaveProfile(base, "work", testCredentials()); err == nil {
		t.Fatal("saved a profile whose service socket cannot bind")
	}
	if _, err := os.Stat(filepath.Dir(base.Env)); !os.IsNotExist(err) {
		t.Fatal("path validation created credential state")
	}
}

func TestDecodeProfileStrictBoundedAndDoesNotEchoInput(t *testing.T) {
	valid := `{"ZELLO_USERNAME":"person","ZELLO_PASSWORD":"  private-marker\\$#\n","ZELLO_CHANNEL":"voice"}`
	c, err := DecodeProfile(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.Network != "sadasant" || c.TranscribeModel != "gpt-4o-mini-transcribe" || c.SpeechModel != "eleven_flash_v2_5" || c.Password != "  private-marker\\$#\n" {
		t.Fatal("defaults or exact credential values changed")
	}
	for _, bad := range []string{
		`{"ZELLO_PASSWORD":"private-marker","ZELLO_PASSWORD":"again"}`,
		`{"ZELLO_PASSWORD":"private-marker","\u005aELLO_PASSWORD":"again"}`,
		`{"private-marker-unknown":"value"}`,
		`{"zello_password":"private-marker"}`,
		`{"ZELLO_PASSWORD":null}`, `{"ZELLO_PASSWORD":123}`, `{"ZELLO_PASSWORD":true}`,
		`{"ZELLO_PASSWORD":[]}`, `{"ZELLO_PASSWORD":{}}`,
		`{"ZELLO_PASSWORD":"private-marker"} {}`, `{"ZELLO_PASSWORD":"private-marker"} extra`,
		`[]`, `null`, ``, `{"ZELLO_PASSWORD":"private-marker`,
		`{"ZELLO_NETWORK":"private-marker/invalid"}`,
		`{"ZELLO_PASSWORD":"` + strings.Repeat("private-marker", 6000) + `"}`,
		"{\"ZELLO_PASSWORD\":\"\xffprivate-marker\"}",
	} {
		_, err := DecodeProfile(strings.NewReader(bad))
		if err == nil {
			t.Fatal("accepted malformed or ambiguous profile JSON")
		}
		if strings.Contains(err.Error(), "private-marker") {
			t.Fatal("decoder disclosed input content")
		}
	}
}

func TestSaveAndLoadProfilesWithoutDefaultOrEnvironmentFallback(t *testing.T) {
	base := profileBase(t)
	if err := Prepare(base); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base.Env, []byte("ZELLO_USERNAME=default-user\nZELLO_PASSWORD=default-password\nOPENAI_API_KEY=default-provider-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ZELLO_USERNAME", "environment-user")
	t.Setenv("OPENAI_API_KEY", "environment-key")
	if _, err := Load(selected(t, base, "missing")); err == nil {
		t.Fatal("an unknown profile silently fell back")
	}
	want := testCredentials()
	want.Password = " leading space $ # \\ \" \ntrailing space "
	if err := SaveProfile(base, "WoRk", want); err != nil {
		t.Fatal(err)
	}
	p := selected(t, base, "work")
	got, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got.Username != want.Username || got.Password != want.Password || got.Channel != want.Channel || got.OpenAIKey != "" || got.Network != "sadasant" || got.Source != p.Env {
		t.Fatal("named credentials changed or inherited another configuration")
	}
	if err := got.ValidateService(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(p.Env); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("credentials are not private: %v, %v", info, err)
	}
	before, err := os.ReadFile(p.Env)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(before, []byte(`"Source"`)) || bytes.Contains(before, []byte(`"Password"`)) || !bytes.Contains(before, []byte(`"ZELLO_PASSWORD"`)) {
		t.Fatal("saved schema differs from documented uppercase names")
	}
	if err := SaveProfile(base, "WORK", Config{Username: "other", Password: "other", Channel: "other"}); err == nil {
		t.Fatal("registration overwrote existing credentials")
	}
	after, err := os.ReadFile(p.Env)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed registration changed existing credentials")
	}
	for _, name := range []string{"", "DEFAULT"} {
		if err := SaveProfile(base, name, want); err == nil {
			t.Fatal("registered the reserved default")
		}
	}
	if err := SaveProfile(base, "incomplete", Config{Password: "private-marker"}); err == nil || strings.Contains(err.Error(), "private-marker") || !strings.Contains(err.Error(), "incomplete.json") {
		t.Fatal("missing required settings did not produce a safe profile-specific error")
	}
}

func TestConcurrentProfileRegistrationPublishesOneCompleteFile(t *testing.T) {
	base := profileBase(t)
	type result struct {
		c   Config
		err error
	}
	results := make(chan result, 12)
	var workers sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			c := testCredentials()
			c.Password = fmt.Sprintf("test-password-%d", i)
			results <- result{c, SaveProfile(base, "shared", c)}
		}(i)
	}
	workers.Wait()
	close(results)
	var winner Config
	wins := 0
	for r := range results {
		if r.err == nil {
			winner = r.c
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("successful registrars = %d, want exactly one", wins)
	}
	got, err := Load(selected(t, base, "shared"))
	if err != nil || got.Password != winner.Password {
		t.Fatal("published credentials did not match the one successful registrar")
	}
	entries, err := os.ReadDir(filepath.Join(filepath.Dir(base.Env), "profiles"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "shared.json" {
		t.Fatal("registration left temporary credential copies")
	}
}

func TestProfilePrepareCreatesPrivateRuntimeOnly(t *testing.T) {
	base := profileBase(t)
	p := selected(t, base, "work")
	if err := Prepare(p); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{filepath.Dir(base.Env), base.Data, filepath.Join(base.Data, "profiles"), p.Data, p.Audio} {
		info, err := os.Stat(dir)
		if err != nil || info.Mode().Perm() != 0700 {
			t.Fatalf("profile directory not private: %v, %v", info, err)
		}
	}
	for _, path := range []string{p.Env, base.Env, p.DB, p.Socket, p.Lock} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("preparing runtime created credentials or opened a service")
		}
	}
	for _, path := range []string{base.Example, p.Log} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("prepared file not private: %v, %v", info, err)
		}
	}
	data, err := os.ReadFile(base.Example)
	if err != nil || string(data) != Example {
		t.Fatal("named prepare did not preserve the global example")
	}
}

func TestProfileCredentialsRejectSymlinksAndLoosePermissions(t *testing.T) {
	for _, target := range []string{"config", "profiles", "credential"} {
		t.Run(target, func(t *testing.T) {
			base := profileBase(t)
			p := selected(t, base, "work")
			outside := t.TempDir()
			link := p.Env
			if target == "config" {
				link = filepath.Dir(base.Env)
			}
			if target == "profiles" {
				link = filepath.Dir(p.Env)
			}
			if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
				t.Fatal(err)
			}
			if target == "credential" {
				outside = filepath.Join(outside, "credential.json")
				b, _ := json.Marshal(testCredentials())
				if err := os.WriteFile(outside, b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(outside, link); err != nil {
				t.Fatal(err)
			}
			if err := SaveProfile(base, "work", testCredentials()); err == nil {
				t.Fatal("registration followed a symlink")
			}
			if _, err := Load(p); err == nil {
				t.Fatal("load followed a symlink")
			}
			if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Fatal("refusal replaced the symlink")
			}
		})
	}
	base := profileBase(t)
	if err := SaveProfile(base, "work", testCredentials()); err != nil {
		t.Fatal(err)
	}
	p := selected(t, base, "work")
	if err := os.Chmod(p.Env, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("accepted world-readable credentials")
	}
	if info, _ := os.Stat(p.Env); info.Mode().Perm() != 0644 {
		t.Fatal("loading silently changed credential permissions")
	}
}

func TestNamedRuntimeRejectsSymlinkDirectories(t *testing.T) {
	for _, location := range []string{"data", "profiles", "profile", "audio"} {
		t.Run(location, func(t *testing.T) {
			base := profileBase(t)
			p := selected(t, base, "work")
			link := map[string]string{"data": base.Data, "profiles": filepath.Dir(p.Data), "profile": p.Data, "audio": p.Audio}[location]
			if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			if err := os.Symlink(outside, link); err != nil {
				t.Fatal(err)
			}
			if err := Prepare(p); err == nil {
				t.Fatal("runtime preparation followed a symlink")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Fatal("runtime wrote outside its selected directory")
			}
		})
	}
}

func TestListProfilesReadsNamesOnlyAndIgnoresUnsafeEntries(t *testing.T) {
	base := profileBase(t)
	if names, err := ListProfiles(base); err != nil || !reflect.DeepEqual(names, []string{"default"}) {
		t.Fatalf("empty profile list = %v, %v", names, err)
	}
	if err := SaveProfile(base, "zebra", testCredentials()); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(filepath.Dir(base.Env), "profiles")
	for _, name := range []string{"alpha.json", "unreadable.json", ".profile-temp", ".unsafe.json", "default.json", "UPPER.json", "white space.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("not JSON; listing must not read credentials"), 0000); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "directory.json"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "zebra.json"), filepath.Join(dir, "link.json")); err != nil {
		t.Fatal(err)
	}
	names, err := ListProfiles(base)
	if err != nil || !reflect.DeepEqual(names, []string{"alpha", "default", "unreadable", "zebra"}) {
		t.Fatalf("listed unexpected names or parsed credentials: %v, %v", names, err)
	}
}

func TestNamedRuntimeRejectsCrossProfileFileAliases(t *testing.T) {
	for _, location := range []string{"database", "wal", "shm", "lock", "log", "socket"} {
		t.Run(location, func(t *testing.T) {
			base := profileBase(t)
			other := selected(t, base, "other")
			p := selected(t, base, "work")
			if err := Prepare(other); err != nil {
				t.Fatal(err)
			}
			if err := Prepare(p); err != nil {
				t.Fatal(err)
			}
			links := map[string]string{"database": p.DB, "wal": p.DB + "-wal", "shm": p.DB + "-shm", "lock": p.Lock, "log": p.Log, "socket": p.Socket}
			link := links[location]
			target := filepath.Join(other.Data, "untouched")
			if err := os.WriteFile(target, []byte("other profile data"), 0600); err != nil {
				t.Fatal(err)
			}
			if location == "log" {
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			if err := Prepare(p); err == nil {
				t.Fatal("runtime preparation accepted a cross-profile file alias")
			}
			data, err := os.ReadFile(target)
			if err != nil || string(data) != "other profile data" {
				t.Fatal("runtime preparation changed another profile's data")
			}
		})
	}
}

func TestLoadIncompleteNamedProfileFailsBeforeRuntimeCreation(t *testing.T) {
	base := profileBase(t)
	if err := SaveProfile(base, "incomplete", testCredentials()); err != nil {
		t.Fatal(err)
	}
	p := selected(t, base, "incomplete")
	if err := os.WriteFile(p.Env, []byte(`{"ZELLO_PASSWORD":"private-marker"}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(p)
	if err == nil || !strings.Contains(err.Error(), "ZELLO_USERNAME") || !strings.Contains(err.Error(), "ZELLO_CHANNEL") || !strings.Contains(err.Error(), "incomplete.json") || strings.Contains(err.Error(), "private-marker") {
		t.Fatal("incomplete named profile was accepted or produced an unsafe diagnostic")
	}
	if _, err := os.Stat(base.Data); !os.IsNotExist(err) {
		t.Fatal("loading an invalid profile created runtime state")
	}
}
