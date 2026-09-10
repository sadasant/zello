package ipc

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"time"
)

// Event is an advisory snapshot of readable incoming IDs, not a consumption or
// an exclusive claim. Consumers must deduplicate IDs and read durable state.
type Event struct {
	Type string   `json:"type"`
	IDs  []string `json:"ids"`
}

func (s *Server) subscribe(parent context.Context, c net.Conn) {
	if s.Inbox == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// No idle timeout. Detect a departing client even when no messages arrive.
	_ = c.SetReadDeadline(time.Time{})
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, c)
		cancel()
		close(done)
	}()
	defer func() { c.Close(); <-done }()
	encoder := json.NewEncoder(c)
	for ctx.Err() == nil {
		// Register before taking the snapshot, so an arrival during the database
		// read schedules another snapshot instead of disappearing in a race.
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		ids, err := s.Inbox(ctx)
		if err != nil {
			return
		}
		if ids == nil {
			ids = []string{}
		}
		// Bound stalled writes, without imposing a deadline on idle subscribers.
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if encoder.Encode(Event{Type: "inbox", IDs: ids}) != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
}

// Subscribe streams the initial unread snapshot and subsequent arrival notices.
// It never polls or consumes. On disconnect it returns an error; reconnecting
// obtains a fresh snapshot, including messages that arrived during downtime.
func Subscribe(ctx context.Context, path string, receive func(Event) error) error {
	c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err = json.NewEncoder(c).Encode(map[string]string{"op": "subscribe"}); err != nil {
		return err
	}
	decoder := json.NewDecoder(c)
	for {
		var event Event
		if err = decoder.Decode(&event); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err = receive(event); err != nil {
			return err
		}
	}
}
