package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/sadasant/zello/internal/config"
	"github.com/sadasant/zello/internal/store"
)

func profileHome(t *testing.T) config.Paths {
	t.Helper()
	// Named-profile Unix sockets must fit Darwin's 103-byte pathname limit.
	home, err := os.MkdirTemp("/tmp", "zp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(home) })
	return config.PathsAt(home)
}

func profileJSON(t *testing.T, name, channel string) string {
	t.Helper()
	data, err := json.Marshal(map[string]string{
		"ZELLO_NETWORK": "test-network", "ZELLO_USERNAME": "test-user-" + name,
		"ZELLO_PASSWORD": "test-password-" + name, "ZELLO_CHANNEL": channel,
		"OPENAI_API_KEY": "test-openai-" + name, "ELEVENLABS_API_KEY": "test-eleven-" + name,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func saveCLIProfile(t *testing.T, base config.Paths, name, channel string) config.Paths {
	t.Helper()
	out, err := run(t, base, profileJSON(t, name, channel), "profile", "save", name, "--stdin", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]string
	if err = json.Unmarshal([]byte(out), &receipt); err != nil || len(receipt) != 1 || receipt["profile"] != strings.ToLower(name) {
		t.Fatalf("unexpected save receipt: %q, %v", out, err)
	}
	p, err := config.SelectProfile(base, name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func writeDefaultProfile(t *testing.T, p config.Paths, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p.Env), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Env, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func profileMessage(t *testing.T, base config.Paths, name, id string) store.Message {
	t.Helper()
	out, err := run(t, base, "", "--profile", name, "show", id)
	if err != nil {
		t.Fatal(err)
	}
	var m store.Message
	if err = json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func profileMessageID(t *testing.T, output string) string {
	t.Helper()
	if strings.HasPrefix(output, "{") {
		var receipt struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(output), &receipt); err != nil || receipt.ID == "" {
			t.Fatalf("invalid queued receipt: %q", output)
		}
		return receipt.ID
	}
	id := strings.TrimSpace(output)
	if id == "" {
		t.Fatal("missing queued ID")
	}
	return id
}

func TestProfileCLISaveCanonicalNameWithoutChangingDefault(t *testing.T) {
	base := profileHome(t)
	defaultText := "ZELLO_CHANNEL=default-channel\nZELLO_USERNAME=default-user\nZELLO_PASSWORD=default-test-password\n"
	writeDefaultProfile(t, base, defaultText)
	p := saveCLIProfile(t, base, "Peter", "peter-channel")
	if p.Profile != "peter" || filepath.Base(p.Env) != "peter.json" {
		t.Fatal("saved profile was not canonicalized")
	}
	info, err := os.Stat(p.Env)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("profile credentials are not private")
	}
	for _, name := range []string{"Peter", "PETER", "peter"} {
		out, err := run(t, base, "", "--profile", name, "count")
		if err != nil || out != "0\n" {
			t.Fatalf("canonical selection %s: %q, %v", name, out, err)
		}
	}
	if _, err := os.Stat(base.DB); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("named selection opened default queue")
	}
	data, err := os.ReadFile(base.Env)
	if err != nil || string(data) != defaultText {
		t.Fatal("named commands changed default credentials")
	}
	out, err := run(t, base, "", "send", "default delivery")
	if err != nil {
		t.Fatal(err)
	}
	m := profileMessage(t, base, "default", profileMessageID(t, out))
	if m.Channel != "default-channel" || m.Text != "default delivery" {
		t.Fatal("default no longer uses .env")
	}
	cfg, err := config.Load(p)
	if err != nil || cfg.Username != "test-user-Peter" || cfg.Channel != "peter-channel" {
		t.Fatal("named selection did not load its saved credentials")
	}
}

func TestProfileCLINamedSelectionIgnoresMalformedDefault(t *testing.T) {
	base := profileHome(t)
	p := saveCLIProfile(t, base, "Peter", "peter-channel")
	writeDefaultProfile(t, base, "ZELLO_PASSWORD=\"unterminated\n")
	out, err := run(t, base, "", "count", "--profile=Peter")
	if err != nil || out != "0\n" {
		t.Fatalf("named profile read malformed .env: %q, %v", out, err)
	}
	if _, err := os.Stat(p.DB); err != nil {
		t.Fatal("named queue not created")
	}
	if out, err = run(t, base, "", "count"); err == nil || out != "" {
		t.Fatal("default stopped reporting its own invalid config")
	}
	if _, err := os.Stat(base.DB); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid default created a queue")
	}
}

func TestProfileCLIUnknownOrCorruptProfileDoesNotFallBackOrCreateQueue(t *testing.T) {
	for _, state := range []string{"missing", "corrupt"} {
		t.Run(state, func(t *testing.T) {
			base := profileHome(t)
			writeDefaultProfile(t, base, "ZELLO_CHANNEL=default-channel\n")
			p, err := config.SelectProfile(base, "Peter")
			if err != nil {
				t.Fatal(err)
			}
			if state == "corrupt" {
				p = saveCLIProfile(t, base, "Peter", "peter-channel")
				if err = os.WriteFile(p.Env, []byte(`{"ZELLO_PASSWORD":{"private":"test-secret"}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			out, err := run(t, base, "", "--profile", "Peter", "send", "must not enter default queue")
			if err == nil || out != "" || strings.Contains(err.Error(), "test-secret") {
				t.Fatalf("unsafe profile failure: %q, %v", out, err)
			}
			for _, path := range []string{base.DB, p.DB, p.Data} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed selection created state at %s", path)
				}
			}
		})
	}
}

func TestProfileCLIQueuesChannelsAndIDsAreIsolated(t *testing.T) {
	base := profileHome(t)
	writeDefaultProfile(t, base, "ZELLO_CHANNEL=default-channel\n")
	paths := map[string]config.Paths{
		"Alice": saveCLIProfile(t, base, "Alice", "alice-channel"),
		"Bob":   saveCLIProfile(t, base, "Bob", "bob-channel"),
	}
	ids := map[string]string{}
	for _, name := range []string{"Alice", "Bob"} {
		out, err := run(t, base, "", "send", "delivery for "+name, "--profile", name, "--json")
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = profileMessageID(t, out)
		m := profileMessage(t, base, strings.ToUpper(name), ids[name])
		if m.Channel != strings.ToLower(name)+"-channel" || m.Status != "queued" || m.Text != "delivery for "+name {
			t.Fatal("message used another profile's identity")
		}
		db, err := store.Open(paths[name].DB)
		if err != nil {
			t.Fatal(err)
		}
		id, err := store.NewID()
		if err == nil {
			err = db.SaveTextIncoming(context.Background(), store.Message{ID: id, Sender: name, Channel: m.Channel, Text: "reply for " + name})
		}
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{{"Alice", "Bob"}, {"Bob", "Alice"}} {
		out, err := run(t, base, "", "show", ids[pair[0]], "--profile", pair[1])
		if !errors.Is(err, store.ErrNotFound) || out != "" {
			t.Fatal("message ID crossed profile queues")
		}
	}
	if out, err := run(t, base, "", "--profile", "Alice", "next"); err != nil || !strings.Contains(out, "reply for Alice") {
		t.Fatalf("Alice consumption: %q, %v", out, err)
	}
	for _, tc := range []struct{ name, count string }{{"ALICE", "0\n"}, {"Bob", "1\n"}, {"default", "0\n"}} {
		if out, err := run(t, base, "", "count", "--profile", tc.name); err != nil || out != tc.count {
			t.Fatalf("profile %s count=%q, err=%v", tc.name, out, err)
		}
	}
}

func TestProfileCLIListDoesNotRevealCredentialsAndSaveDoesNotClobber(t *testing.T) {
	base := profileHome(t)
	_ = saveCLIProfile(t, base, "Zulu", "zulu-channel")
	p := saveCLIProfile(t, base, "Peter", "peter-channel")
	before, err := os.ReadFile(p.Env)
	if err != nil {
		t.Fatal(err)
	}
	out, err := run(t, base, profileJSON(t, "replacement", "other-channel"), "profile", "save", "PETER", "--stdin")
	if err == nil || out != "" {
		t.Fatal("duplicate registration succeeded")
	}
	after, err := os.ReadFile(p.Env)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("duplicate registration altered existing credentials")
	}
	writeDefaultProfile(t, base, "ZELLO_PASSWORD=\"unterminated\n")
	out, err = run(t, base, "", "profile", "list", "--json")
	var names []string
	if err != nil || json.Unmarshal([]byte(out), &names) != nil || !reflect.DeepEqual(names, []string{"default", "peter", "zulu"}) {
		t.Fatalf("unexpected profile list: %q, %v", out, err)
	}
	if out, err = run(t, base, "", "profile", "list"); err != nil || out != "default\npeter\nzulu\n" {
		t.Fatalf("list exposed more than names: %q, %v", out, err)
	}
	for _, secret := range []string{"test-password", "test-openai", "test-eleven", "test-user"} {
		if strings.Contains(out, secret) {
			t.Fatal("profile list exposed credentials")
		}
	}
}

func TestProfileCLISelectorPositionAndLiteralBoundary(t *testing.T) {
	base := profileHome(t)
	writeDefaultProfile(t, base, "ZELLO_CHANNEL=default-channel\n")
	_ = saveCLIProfile(t, base, "Peter", "peter-channel")
	cases := []struct {
		args          []string
		profile, text string
	}{
		{[]string{"--profile", "Peter", "send", "before", "--json"}, "peter", "before"},
		{[]string{"send", "after", "--profile", "Peter", "--json"}, "peter", "after"},
		{[]string{"send", "--profile=Peter", "equals", "--json"}, "peter", "equals"},
		{[]string{"--profile=Peter", "send", "--", "--profile", "--json"}, "peter", "--profile --json"},
		{[]string{"send", "--profile", "Peter", "--", "--profile=other", "--json"}, "peter", "--profile=other --json"},
		{[]string{"--json", "--profile", "Peter", "send", "--", "--", "--profile"}, "peter", "-- --profile"},
		{[]string{"--profile", "Peter", "send", "--", "--profile", "Other", "--profile=Else"}, "peter", "--profile Other --profile=Else"},
		{[]string{"send", "--", "--profile", "Peter"}, "default", "--profile Peter"},
	}
	for _, tc := range cases {
		out, err := run(t, base, "", tc.args...)
		if err != nil {
			t.Fatalf("selector arguments %v: %v", tc.args, err)
		}
		m := profileMessage(t, base, tc.profile, profileMessageID(t, out))
		if m.Text != tc.text || m.Channel != tc.profile+"-channel" {
			t.Fatalf("selector changed delivery: got %q on %q", m.Text, m.Channel)
		}
	}
}

func TestProfileCLIInvalidSelectorsFailBeforeStateCreation(t *testing.T) {
	cases := [][]string{
		{"--profile"}, {"send", "--profile"}, {"--profile=", "count"}, {"--profile", "", "count"},
		{"--profile", "--json", "count"}, {"--profile", "Peter", "count", "--profile=Peter"},
		{"--profile=Peter", "--profile", "Other", "count"}, {"--profile", "../Peter", "count"},
		{"--profile", "/tmp/Peter", "count"}, {"--profile", "a/b", "count"}, {"--profile", ".", "count"},
		{"--profile", "Kelvin", "count"}, {"--profile", strings.Repeat("a", 33), "count"},
		{"--profile", "Peter", "profile", "list"}, {"profile", "save", "default", "--stdin"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			base := profileHome(t)
			out, err := run(t, base, "", args...)
			if err == nil || out != "" {
				t.Fatalf("invalid selector accepted: %q, %v", out, err)
			}
			if _, err := os.Stat(base.Data); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid selector created runtime data")
			}
		})
	}
}
