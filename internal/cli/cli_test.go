package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sadasant/zello/internal/config"
	"github.com/sadasant/zello/internal/store"
)

func setup(t *testing.T) (config.Paths, *store.Store) {
	t.Helper()
	p := config.PathsAt(t.TempDir())
	if err := config.Prepare(p); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Env, []byte("ZELLO_CHANNEL=agent\n"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(p.DB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return p, db
}
func run(t *testing.T, p config.Paths, input string, args ...string) (string, error) {
	t.Helper()
	var out, diag bytes.Buffer
	err := Run(context.Background(), args, strings.NewReader(input), &out, &diag, p)
	if diag.Len() != 0 {
		t.Fatalf("unexpected diagnostics: %s", diag.String())
	}
	return out.String(), err
}
func ready(t *testing.T, db *store.Store, text string) string {
	t.Helper()
	id, err := store.NewID()
	if err != nil {
		t.Fatal(err)
	}
	if err = db.SaveIncoming(context.Background(), store.Message{ID: id, Sender: "daniel", Channel: "agent", AudioPath: "private.opus"}); err != nil {
		t.Fatal(err)
	}
	if err = db.CompleteIncoming(context.Background(), id, text); err != nil {
		t.Fatal(err)
	}
	return id
}
func TestDurableCLILifecycle(t *testing.T) {
	p, db := setup(t)
	id := ready(t, db, "Can you hear me?")
	output, err := run(t, p, "", "count")
	if err != nil || output != "1\n" {
		t.Fatalf("count %q %v", output, err)
	}
	output, err = run(t, p, "", "peek", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var msg store.Message
	if err = json.Unmarshal([]byte(output), &msg); err != nil {
		t.Fatal(err)
	}
	if msg.ID != id || msg.Text != "Can you hear me?" || msg.Status != "unread" || strings.Contains(output, "audio_path") {
		t.Fatalf("peek %s", output)
	}
	output, err = run(t, p, "", "next", "--json")
	if err != nil || !strings.Contains(output, `"status":"consumed"`) {
		t.Fatalf("next %q %v", output, err)
	}
	if _, err = run(t, p, "", "consume", id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double consume: %v", err)
	}
	output, err = run(t, p, "", "count")
	if err != nil || output != "0\n" {
		t.Fatalf("count %q %v", output, err)
	}
	if _, err = run(t, p, "", "peek"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("empty peek: %v", err)
	}
	output, err = run(t, p, "", "inbox", "--json")
	if err != nil || output != "[]\n" {
		t.Fatalf("empty inbox %q %v", output, err)
	}
	id2 := ready(t, db, "Second message")
	if _, err = run(t, p, "", "consume", id2); err != nil {
		t.Fatal(err)
	}
	output, err = run(t, p, "", "show", id2)
	if err != nil || !strings.Contains(output, "Second message") {
		t.Fatalf("show %q %v", output, err)
	}
}
func TestSendAndStatusWithoutService(t *testing.T) {
	p, db := setup(t)
	output, err := run(t, p, "hello from stdin\n", "send")
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(output)
	msg, err := db.Show(context.Background(), id)
	if err != nil || msg.Status != "queued" || msg.Text != "hello from stdin" {
		t.Fatalf("queued %+v %v", msg, err)
	}
	if _, err = run(t, p, "\n", "send"); err == nil {
		t.Fatal("accepted empty send")
	}
	output, err = run(t, p, "", "status", "--json")
	if !errors.Is(err, ErrDisconnected) || !strings.Contains(output, `"service_running":false`) {
		t.Fatalf("status %q %v", output, err)
	}
}
func TestWaitPollingConsumesAndCancels(t *testing.T) {
	p, db := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- Run(ctx, []string{"wait", "--json"}, strings.NewReader(""), &out, &bytes.Buffer{}, p) }()
	select {
	case err := <-done:
		t.Fatalf("wait returned before incoming message: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	id := ready(t, db, "Wake up")
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !strings.Contains(out.String(), id) {
		t.Fatalf("wait output %s", out.String())
	}
	if n, _ := db.Count(ctx); n != 0 {
		t.Fatalf("remaining unread %d", n)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := Run(canceled, []string{"wait"}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation %v", err)
	}
}

func TestSendRejectsTruncatedStdinAndCancelsBlockedRead(t *testing.T) {
	p, _ := setup(t)
	if _, err := run(t, p, "hello"+strings.Repeat(" ", 160000)+"important suffix", "send"); err == nil {
		t.Fatal("accepted truncated stdin")
	}
	reader, writer := io.Pipe()
	defer writer.Close()
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, []string{"send"}, reader, &bytes.Buffer{}, &bytes.Buffer{}, p) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled send %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send ignored cancellation")
	}
}
