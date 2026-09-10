package ipc

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func eventResult(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case e := <-events:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not deliver")
		return Event{}
	}
}

func TestSubscriptionSnapshotRaceBroadcastAndReconnect(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	s, path := start(t, func(s *Server) {
		s.Inbox = func(ctx context.Context) ([]string, error) {
			if reads.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
					return nil, nil // Snapshot just before an arrival.
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []string{"one"}, nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan Event, 8)
	done := make(chan error, 2)
	subscribe := func() {
		done <- Subscribe(ctx, path, func(e Event) error { events <- e; return nil })
	}
	go subscribe()
	await(t, entered)
	s.Notify()
	close(release)
	if e := eventResult(t, events); e.Type != "inbox" || e.IDs == nil || len(e.IDs) != 0 {
		t.Fatalf("initial snapshot: %+v", e)
	}
	if e := eventResult(t, events); len(e.IDs) != 1 || e.IDs[0] != "one" {
		t.Fatalf("arrival during query lost: %+v", e)
	}
	go subscribe()
	eventResult(t, events) // New subscriber independently gets existing unread.
	s.Notify()
	eventResult(t, events)
	eventResult(t, events)
	cancel()
	for range 2 {
		if err := result(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	}
	// Reconnect still sees the same unread message; subscriptions do not consume.
	err := Subscribe(context.Background(), path, func(e Event) error {
		if len(e.IDs) != 1 || e.IDs[0] != "one" {
			t.Errorf("reconnect lost unread: %+v", e)
		}
		return io.ErrClosedPipe
	})
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("consumer output failure: %v", err)
	}
}

func TestSubscriptionHasNoIdleRefreshOrDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("crosses the old 35-second connection deadline")
	}
	var reads atomic.Int32
	s, path := start(t, func(s *Server) {
		s.Inbox = func(context.Context) ([]string, error) { reads.Add(1); return nil, nil }
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan Event, 2)
	done := make(chan error, 1)
	go func() { done <- Subscribe(ctx, path, func(e Event) error { events <- e; return nil }) }()
	eventResult(t, events)
	select {
	case <-events:
		t.Fatal("idle subscription emitted a timed refresh")
	case err := <-done:
		t.Fatalf("idle subscription disconnected: %v", err)
	case <-time.After(36 * time.Second):
	}
	if reads.Load() != 1 {
		t.Fatalf("idle database polling: %d reads", reads.Load())
	}
	s.Notify()
	eventResult(t, events)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := result(t, done); err == nil {
		t.Fatal("server shutdown must end the stream")
	}
}

func TestSubscriptionDisconnectCancelsSnapshot(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	_, path := start(t, func(s *Server) {
		s.Inbox = func(ctx context.Context) ([]string, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Subscribe(ctx, path, func(Event) error { return nil }) }()
	await(t, entered)
	cancel()
	await(t, canceled)
	if err := result(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
