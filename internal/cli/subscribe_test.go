package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sadasant/zello/internal/ipc"
)

func TestSubscribeCLIEmitsOnlyIDsWithoutConsuming(t *testing.T) {
	p, db := setup(t)
	id := ready(t, db, "Private incoming text")
	dir, err := os.MkdirTemp("/tmp", "zcsub-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	p.Socket, p.Lock = filepath.Join(dir, "z.sock"), filepath.Join(dir, "z.lock")
	s, err := ipc.Listen(p.Socket, p.Lock)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Inbox = db.InboxIDs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go s.Serve(ctx)
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		err := Run(ctx, []string{"subscribe", "--json"}, strings.NewReader(""), writer, io.Discard, p)
		writer.CloseWithError(err)
		done <- err
	}()
	var event map[string]any
	if err := json.NewDecoder(reader).Decode(&event); err != nil {
		t.Fatal(err)
	}
	ids, ok := event["ids"].([]any)
	if len(event) != 2 || event["type"] != "inbox" || !ok || len(ids) != 1 || ids[0] != id {
		t.Fatalf("unexpected output fields: %+v", event)
	}
	if count, err := db.Count(ctx); err != nil || count != 1 {
		t.Fatalf("subscription consumed message: %d, %v", count, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("CLI did not cancel")
	}
}
