package ipc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func paths(t *testing.T) (string, string) {
	t.Helper()
	// macOS has a short sun_path; testing.T.TempDir can exceed it.
	dir, err := os.MkdirTemp("/tmp", "zipc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "z.sock"), filepath.Join(dir, "z.lock")
}

func start(t *testing.T, configure func(*Server)) (*Server, string) {
	t.Helper()
	path, lock := paths(t)
	s, err := Listen(path, lock)
	if err != nil {
		t.Fatal(err)
	}
	configure(s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Serve did not stop")
		}
	})
	return s, path
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not complete")
	}
}

func result(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("request did not complete")
		return nil
	}
}

func TestStatusWakeAndAlreadyUnread(t *testing.T) {
	var wakes atomic.Int32
	_, path := start(t, func(s *Server) {
		s.State = func() Status { return Status{State: "connected", Running: true, NativeTranscription: true} }
		s.Wake = func() { wakes.Add(1) }
		s.Unread = func(context.Context) (bool, error) { return true, nil }
	})
	var state Status
	if err := Request(context.Background(), path, "status", &state); err != nil {
		t.Fatal(err)
	}
	if state.State != "connected" || !state.Running || !state.NativeTranscription {
		t.Fatalf("status = %+v", state)
	}
	if err := Request(context.Background(), path, "wake", &struct{}{}); err != nil || wakes.Load() != 1 {
		t.Fatalf("wake = %d, %v", wakes.Load(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := Request(ctx, path, "wait", &struct{}{}); err != nil {
		t.Fatalf("wait with existing unread message blocked: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0600 {
		t.Fatalf("socket must be private: %v, %v", fi, err)
	}
}

func TestWaitCannotLoseNotificationDuringUnreadQuery(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s, path := start(t, func(s *Server) {
		s.Unread = func(ctx context.Context) (bool, error) {
			close(entered)
			select {
			case <-release:
				return false, nil // Represents a query snapshot before the new row.
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
	})
	done := make(chan error, 1)
	go func() { done <- Request(context.Background(), path, "wait", &struct{}{}) }()
	await(t, entered)
	s.Notify()
	close(release)
	if err := result(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestNotifyBroadcastsToAllWaiters(t *testing.T) {
	const count = 12
	entered := make(chan struct{}, count)
	s, path := start(t, func(s *Server) {
		s.Unread = func(context.Context) (bool, error) { entered <- struct{}{}; return false, nil }
	})
	done := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() { done <- Request(context.Background(), path, "wait", &struct{}{}) }()
	}
	for i := 0; i < count; i++ {
		await(t, entered)
	}
	s.Notify()
	for i := 0; i < count; i++ {
		if err := result(t, done); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientCancellationAndServerCloseUnblockWait(t *testing.T) {
	for _, closeServer := range []bool{false, true} {
		t.Run(map[bool]string{false: "client cancellation", true: "server close"}[closeServer], func(t *testing.T) {
			entered := make(chan struct{})
			s, path := start(t, func(s *Server) {
				s.Unread = func(ctx context.Context) (bool, error) {
					close(entered)
					if closeServer {
						<-ctx.Done()
						return false, ctx.Err()
					}
					return false, nil
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Request(ctx, path, "wait", &struct{}{}) }()
			await(t, entered)
			if closeServer {
				closed := make(chan error, 1)
				go func() { closed <- s.Close() }()
				if err := result(t, closed); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			if err := result(t, done); err == nil || (!closeServer && !errors.Is(err, context.Canceled)) {
				t.Fatalf("cancellation result = %v", err)
			}
		})
	}
}

func TestSingletonAndSocketRecovery(t *testing.T) {
	t.Run("singleton", func(t *testing.T) {
		path, lock := paths(t)
		s, err := Listen(path, lock)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if other, err := Listen(path, lock); err == nil {
			other.Close()
			t.Fatal("second service acquired live lock")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = Listen(path, lock)
		if err != nil {
			t.Fatalf("closed service retained its lock: %v", err)
		}
		s.Close()
	})
	t.Run("stale socket", func(t *testing.T) {
		path, lock := paths(t)
		l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		l.SetUnlinkOnClose(false)
		l.Close()
		s, err := Listen(path, lock)
		if err != nil {
			t.Fatalf("stale socket was not recovered: %v", err)
		}
		s.Close()
	})
	t.Run("non socket preserved", func(t *testing.T) {
		path, lock := paths(t)
		if err := os.WriteFile(path, []byte("keep this"), 0600); err != nil {
			t.Fatal(err)
		}
		if s, err := Listen(path, lock); err == nil {
			s.Close()
			t.Fatal("non socket path was replaced")
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "keep this" {
			t.Fatalf("file changed: %q, %v", data, err)
		}
		os.Remove(path)
		s, err := Listen(path, lock)
		if err != nil {
			t.Fatalf("failed Listen leaked its lock: %v", err)
		}
		s.Close()
	})
	t.Run("unrelated live socket preserved", func(t *testing.T) {
		path, lock := paths(t)
		l, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		if s, err := Listen(path, lock); err == nil {
			s.Close()
			t.Fatal("unrelated live socket was replaced")
		}
		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("existing socket damaged: %v", err)
		}
		c.Close()
	})
}

func TestConcurrentCloseServeAndAccept(t *testing.T) {
	for iteration := 0; iteration < 25; iteration++ {
		path, lock := paths(t)
		s, err := Listen(path, lock)
		if err != nil {
			t.Fatal(err)
		}
		s.Unread = func(context.Context) (bool, error) { return false, nil }
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := net.DialTimeout("unix", path, time.Second)
				if err == nil {
					defer c.Close()
					// Exercise a handler still blocked decoding its request.
					_, _ = c.Write([]byte(strings.Repeat(" ", 8)))
				}
			}()
		}
		serve := make(chan error, 1)
		go func() { serve <- s.Serve(ctx) }()
		closed := make(chan error, 2)
		go func() { closed <- s.Close() }()
		go func() { closed <- s.Close() }()
		cancel()
		for i := 0; i < 2; i++ {
			if err := result(t, closed); err != nil {
				t.Fatal(err)
			}
		}
		if err := result(t, serve); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		s.Notify() // A notifier racing with or following shutdown must be harmless.
	}
}
