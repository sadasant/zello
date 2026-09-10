package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

func openTest(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func incoming(t *testing.T, s *Store, id string, at time.Time, done bool) {
	t.Helper()
	if err := s.SaveIncoming(ctx, Message{ID: id, Sender: "person", Channel: "channel", AudioPath: id + ".ogg", CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if done {
		if err := s.CompleteIncoming(ctx, id, "text "+id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTextIncomingCommittedReadyAndSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.db")
	s := openTest(t, path)
	m := Message{
		ID: "typed", Sender: "person", Channel: "channel", ReplyTo: "previous",
		Text: "  First line\nSecond line.\n", CreatedAt: time.Now().UTC(),
	}
	if err := s.SaveTextIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen before any follow-up completion: the first committed state must
	// already contain the original text and be available to a CLI consumer.
	s = openTest(t, path)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || got.Text != m.Text || got.Sender != m.Sender || got.Channel != m.Channel || got.ReplyTo != m.ReplyTo || !got.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("typed content or metadata changed: %+v", got)
	}
	if got.Direction != "incoming" || got.Status != "unread" || got.TranscriptionStatus != "done" || got.AudioPath != "" || got.TranscriptionAttempts != 0 || !got.NextTranscriptionAt.IsZero() {
		t.Fatalf("typed message requires an audio completion step: %+v", got)
	}
	if n, err := s.Count(ctx); err != nil || n != 1 {
		t.Fatalf("typed message not immediately readable: %d, %v", n, err)
	}
	if pending, err := s.PendingIncoming(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("typed message created an audio job: %+v, %v", pending, err)
	}
	if err := s.SaveTextIncoming(ctx, Message{ID: m.ID, Channel: m.Channel, Text: "duplicate body"}); err == nil {
		t.Fatal("duplicate ID overwrote the original text")
	}
	if err := s.CompleteIncoming(ctx, m.ID, "late transcription"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("typed body could be overwritten by transcription: %v", err)
	}
	got, err = s.Next(ctx)
	if err != nil || got.Text != m.Text || got.Status != "consumed" {
		t.Fatalf("typed message was not consumed intact: %+v, %v", got, err)
	}
}

func TestTextIncomingRejectsEmptyMessagesWithoutLeavingRows(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "messages.db"))
	for _, m := range []Message{{ID: "empty"}, {ID: "whitespace", Text: " \t\n "}, {Text: "missing ID"}} {
		if err := s.SaveTextIncoming(ctx, m); err == nil {
			t.Fatalf("accepted invalid text message %+v", m)
		}
		if _, err := s.Show(ctx, m.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("invalid message left a durable row: %v", err)
		}
	}
}

func TestInboxWaitsForTranscriptionAndConsumptionIsFinal(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "messages.db"))
	now := time.Now().UTC()
	incoming(t, s, "pending", now, false)
	incoming(t, s, "later", now.Add(time.Second), true)
	incoming(t, s, "oldest", now.Add(-time.Second), true)
	if _, err := s.Enqueue(ctx, "outgoing", "channel"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Count(ctx); err != nil || n != 2 {
		t.Fatalf("count = %d, %v", n, err)
	}
	if err := s.Consume(ctx, "pending"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("consumed incomplete transcript: %v", err)
	}
	for i := 0; i < 2; i++ {
		m, err := s.Peek(ctx)
		if err != nil || m.ID != "oldest" || m.Status != "unread" || m.ConsumedAt != nil {
			t.Fatalf("peek changed unread: %+v, %v", m, err)
		}
	}
	m, err := s.Next(ctx)
	if err != nil || m.ID != "oldest" || m.ConsumedAt == nil || m.Status != "consumed" {
		t.Fatalf("next = %+v, %v", m, err)
	}
	if err := s.CompleteIncoming(ctx, "oldest", "late native text"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replaced consumed transcript: %v", err)
	}
	if err := s.CompleteIncoming(ctx, "later", "late native text"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replaced completed transcript: %v", err)
	}
	if err := s.Consume(ctx, "oldest"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double consume = %v", err)
	}
	if err := s.Consume(ctx, "later"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty next = %v", err)
	}
	if items, err := s.Inbox(ctx); err != nil || items == nil || len(items) != 0 {
		t.Fatalf("empty inbox should encode as []: %v, %v", items, err)
	}
	if err := s.CompleteIncoming(ctx, "pending", "now ready"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Count(ctx); err != nil || n != 1 {
		t.Fatalf("completed count = %d, %v", n, err)
	}
}

func TestRestartPreservesQueuesAndOriginalAudio(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory with # spaces", "messages.db")
	s := openTest(t, path)
	now := time.Now().UTC()
	incoming(t, s, "pending", now, false)
	incoming(t, s, "ready", now, true)
	incoming(t, s, "consumed", now, true)
	if err := s.Consume(ctx, "consumed"); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, status := range []string{"queued", "synthesizing", "sending", "sent", "failed"} {
		m, err := s.Enqueue(ctx, status, "channel")
		if err != nil {
			t.Fatal(err)
		}
		ids[status] = m.ID
		if err := s.SetOutgoing(ctx, m.ID, status, m.ID+".ogg", ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for original, expected := range map[string]string{"queued": "queued", "synthesizing": "queued", "sending": "failed", "sent": "sent", "failed": "failed"} {
		m, err := s.Show(ctx, ids[original])
		if err != nil || m.Status != expected || m.AudioPath != m.ID+".ogg" {
			t.Fatalf("recover %s: %+v, %v", original, m, err)
		}
		if original == "sending" && !strings.Contains(m.Error, "unknown") {
			t.Fatal("interrupted delivery must report uncertainty")
		}
	}
	if n, err := s.Count(ctx); err != nil || n != 1 {
		t.Fatalf("restart count = %d, %v", n, err)
	}
	pending, err := s.PendingIncoming(ctx)
	if err != nil || len(pending) != 1 || pending[0].ID != "pending" || pending[0].AudioPath != "pending.ogg" {
		t.Fatalf("pending audio lost: %+v, %v", pending, err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0600 {
		t.Fatalf("database permissions: %v, %v", fi, err)
	}
}

func TestTranscriptionGraceBackoffAndBatchLimit(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "messages.db"))
	now := time.Now().UTC()
	if err := s.SaveIncoming(ctx, Message{ID: "native-grace", Channel: "channel", NextTranscriptionAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.PendingIncoming(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("native grace not honored: %+v, %v", pending, err)
	}
	incoming(t, s, "retry", now, false)
	if err := s.FailIncoming(ctx, "retry", "transcription unavailable"); err != nil {
		t.Fatal(err)
	}
	m, err := s.Show(ctx, "retry")
	if err != nil || m.TranscriptionAttempts != 1 || !m.NextTranscriptionAt.After(now) || m.Error == "" || m.TranscriptionStatus != "pending" {
		t.Fatalf("retry not durable: %+v, %v", m, err)
	}
	if pending, err := s.PendingIncoming(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("failed transcript retried immediately: %+v, %v", pending, err)
	}
	if err := s.CompleteIncoming(ctx, "retry", "late native transcript"); err != nil {
		t.Fatal(err)
	}
	if err := s.FailIncoming(ctx, "retry", "late worker failure"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed transcript invalidated: %v", err)
	}
	for i := 0; i < 20; i++ {
		incoming(t, s, fmt.Sprintf("batch-%02d", i), now, false)
	}
	if pending, err := s.PendingIncoming(ctx); err != nil || len(pending) != 16 {
		t.Fatalf("unbounded transcription batch: %d, %v", len(pending), err)
	}
}

func TestBrokenIncomingAudioIsRetainedWithoutRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.db")
	s := openTest(t, path)
	incoming(t, s, "broken", time.Now(), false)
	if err := s.RejectIncoming(ctx, "broken", "stream ended before final audio packet"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := s.Show(ctx, "broken")
	if err != nil || m.Status != "unread" || m.TranscriptionStatus != "failed" || m.AudioPath != "broken.ogg" || m.Error == "" {
		t.Fatalf("incomplete audio not retained: %+v, %v", m, err)
	}
	if pending, err := s.PendingIncoming(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("broken audio scheduled for retry: %+v, %v", pending, err)
	}
	if count, err := s.Count(ctx); err != nil || count != 0 {
		t.Fatalf("broken audio visible to consumers: %d, %v", count, err)
	}
	if err := s.CompleteIncoming(ctx, "broken", "late partial text"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("permanent rejection undone: %v", err)
	}
	if err := s.Consume(ctx, "broken"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("consumed rejected audio: %v", err)
	}
	incoming(t, s, "ready", time.Now(), true)
	if err := s.RejectIncoming(ctx, "ready", "late failure"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ready message invalidated: %v", err)
	}
}

func TestConcurrentConsumersAndSendersAcrossIndependentHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.db")
	var stores []*Store
	for i := 0; i < 8; i++ {
		stores = append(stores, openTest(t, path))
	}
	now := time.Now().UTC()
	const messages = 64
	for i := 0; i < messages; i++ {
		incoming(t, stores[0], fmt.Sprintf("in-%02d", i), now, true)
		if _, err := stores[0].Enqueue(ctx, fmt.Sprintf("out-%02d", i), "channel"); err != nil {
			t.Fatal(err)
		}
	}
	for _, outgoing := range []bool{false, true} {
		seen := make(map[string]bool)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, s := range stores {
			wg.Add(1)
			go func(s *Store) {
				defer wg.Done()
				for {
					var m Message
					var err error
					if outgoing {
						m, err = s.ClaimOutgoing(ctx)
					} else {
						m, err = s.Next(ctx)
					}
					if errors.Is(err, ErrNotFound) {
						return
					}
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					if seen[m.ID] {
						t.Errorf("message %s claimed twice", m.ID)
					}
					seen[m.ID] = true
					mu.Unlock()
				}
			}(s)
		}
		wg.Wait()
		if len(seen) != messages {
			t.Fatalf("outgoing=%v: claimed %d of %d", outgoing, len(seen), messages)
		}
	}
	incoming(t, stores[0], "consume-race", now, true)
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for _, s := range stores {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			err := s.Consume(ctx, "consume-race")
			if err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			} else if !errors.Is(err, ErrNotFound) {
				t.Error(err)
			}
		}(s)
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("consume winners = %d", winners)
	}
}
