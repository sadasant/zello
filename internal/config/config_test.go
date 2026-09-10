package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPathsStayAtSpecifiedHomeLocations(t *testing.T) {
	home := t.TempDir()
	got := PathsAt(home)
	want := Paths{
		Env: filepath.Join(home, ".config/zello/.env"), Example: filepath.Join(home, ".config/zello/.env.example"),
		Data: filepath.Join(home, ".local/share/zello"), DB: filepath.Join(home, ".local/share/zello/messages.db"),
		Audio: filepath.Join(home, ".local/share/zello/audio"), Log: filepath.Join(home, ".local/share/zello/zello.log"),
		Socket: filepath.Join(home, ".local/share/zello/zello.sock"), Lock: filepath.Join(home, ".local/share/zello/service.lock"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("paths do not match the specified layout")
	}
	actualHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	actual, err := UserPaths()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, PathsAt(actualHome)) {
		t.Fatal("configuration unexpectedly followed XDG overrides")
	}
}

func TestPrepareCreatesPrivatePathsWithoutCreatingCredentials(t *testing.T) {
	p := PathsAt(t.TempDir())
	if err := Prepare(p); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{filepath.Dir(p.Env), p.Data, p.Audio} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0700 {
			t.Fatalf("directory has mode %v", info.Mode())
		}
	}
	for _, path := range []string{p.Example, p.Log} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("file has mode %v", info.Mode())
		}
	}
	if _, err := os.Stat(p.Env); !os.IsNotExist(err) {
		t.Fatal("Prepare created or touched a real credential file")
	}
	b, err := os.ReadFile(p.Example)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != Example {
		t.Fatal("example differs from documented settings")
	}
	if err := os.WriteFile(p.Example, []byte("# User annotation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Prepare(p); err != nil {
		t.Fatal(err)
	}
	b, err = os.ReadFile(p.Example)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "# User annotation\n" {
		t.Fatal("repeat Prepare replaced the user's example")
	}
}

func TestLoadDefaultsAndReloadsOnlyTheConfiguredFile(t *testing.T) {
	p := PathsAt(t.TempDir())
	if err := Prepare(p); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ZELLO_NETWORK", "unrelated-process-value")
	t.Setenv("OPENAI_API_KEY", "fake-process-key")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Network != "sadasant" || c.TranscribeModel != "gpt-4o-mini-transcribe" || c.SpeechModel != "eleven_flash_v2_5" || c.OpenAIKey != "" {
		t.Fatal("missing-file defaults or process isolation incorrect")
	}
	first := "ZELLO_NETWORK=network-a\nZELLO_USERNAME=alice\nZELLO_PASSWORD='fake$Password#one'\nZELLO_CHANNEL=voice\nOPENAI_API_KEY=fake-file-key\nOPENAI_TRANSCRIBE_MODEL=model-a\nELEVENLABS_API_KEY=fake-speech-key\nELEVENLABS_VOICE_ID=voice-a\nELEVENLABS_MODEL_ID=speech-a\n"
	if err := os.WriteFile(p.Env, []byte(first), 0600); err != nil {
		t.Fatal(err)
	}
	c, err = Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Network != "network-a" || c.Username != "alice" || c.Password != "fake$Password#one" || c.Channel != "voice" || c.OpenAIKey != "fake-file-key" || c.TranscribeModel != "model-a" || c.ElevenLabsKey != "fake-speech-key" || c.VoiceID != "voice-a" || c.SpeechModel != "speech-a" {
		t.Fatal("file values were not loaded exactly")
	}
	if c.Endpoint() != "wss://zellowork.io/ws/network-a" {
		t.Fatal("incorrect Work endpoint")
	}
	if err := os.WriteFile(p.Env, []byte("ZELLO_USERNAME=bob\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err = Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Username != "bob" || c.Password != "" || c.OpenAIKey != "" || c.Network != "sadasant" {
		t.Fatal("configuration was cached across calls")
	}
}

func TestLoadErrorsDoNotEchoMalformedValues(t *testing.T) {
	p := PathsAt(t.TempDir())
	if err := Prepare(p); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"ZELLO_PASSWORD=\"private-marker-unclosed", "ZELLO_NETWORK=private-marker/other", "ZELLO_NETWORK=https://private-marker.example"} {
		if err := os.WriteFile(p.Env, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(p)
		if err == nil {
			t.Fatal("accepted invalid config")
		}
		if strings.Contains(err.Error(), "private-marker") {
			t.Fatal("parse error disclosed a configuration value")
		}
	}
}

func TestValidateServiceNamesOnlyMissingSettings(t *testing.T) {
	c := Config{Username: "fake-private-user", Password: "fake-private-password"}
	err := c.ValidateService()
	if err == nil || !strings.Contains(err.Error(), "ZELLO_CHANNEL") {
		t.Fatal("missing channel was not identified")
	}
	if strings.Contains(err.Error(), "fake-private") {
		t.Fatal("validation disclosed credentials")
	}
	c.Channel = "voice"
	if err := c.ValidateService(); err != nil {
		t.Fatal(err)
	} // Provider keys are optional until their operation is needed.
}
