// Package ipc provides a private Unix socket for live health and queue notifications.
package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

type Status struct {
	State               string `json:"state"`
	Running             bool   `json:"service_running"`
	NativeTranscription bool   `json:"native_transcription_observed"`
}

type Server struct {
	listener  net.Listener
	lock      *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	changed   chan struct{}
	closing   bool
	serving   bool
	conns     map[net.Conn]struct{}
	clients   sync.WaitGroup
	stopOnce  sync.Once
	closeOnce sync.Once
	closeErr  error

	// Set callbacks before calling Serve. They must respect cancellation and must
	// not call Close synchronously from a request handler.
	State  func() Status
	Unread func(context.Context) (bool, error)
	Inbox  func(context.Context) ([]string, error)
	Wake   func()
}

func Listen(socket, lockPath string) (*Server, error) {
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*Server, error) { lock.Close(); return nil, e }
	if err = lock.Chmod(0600); err != nil {
		return fail(err)
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(errors.New("zello service is already running"))
	}
	if info, err := os.Lstat(socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fail(errors.New("socket path is occupied by a non-socket file"))
		}
		// The lock is authoritative for our service, but do not unlink an active
		// socket owned by another program or by an accidentally different lock.
		if conn, err := net.DialTimeout("unix", socket, time.Second); err == nil {
			conn.Close()
			return fail(errors.New("socket path is already serving connections"))
		}
		if err = os.Remove(socket); err != nil {
			return fail(err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		return fail(err)
	}
	if err = os.Chmod(socket, 0600); err != nil {
		l.Close()
		return fail(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{listener: l, lock: lock, ctx: ctx, cancel: cancel,
		changed: make(chan struct{}), conns: make(map[net.Conn]struct{})}, nil
}

func (s *Server) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closing {
		close(s.changed)
		s.changed = make(chan struct{})
	}
}

func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	if s.serving {
		s.mu.Unlock()
		return errors.New("IPC server is already serving")
	}
	s.serving = true
	s.mu.Unlock()
	stop := context.AfterFunc(ctx, s.shutdown)
	defer stop()
	defer func() { s.shutdown(); s.clients.Wait() }()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		// The closing flag and Add share the same lock. Close can begin waiting
		// only after it has prevented every future Add.
		s.clients.Add(1)
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go func() {
			defer s.clients.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
				conn.Close()
			}()
			s.handle(s.ctx, conn)
		}()
	}
}

func (s *Server) shutdown() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closing = true
		s.cancel()
		close(s.changed)
		s.listener.Close()
		for conn := range s.conns {
			conn.Close()
		}
	})
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.shutdown()
		s.clients.Wait()
		s.closeErr = s.lock.Close()
	})
	return s.closeErr
}

func (s *Server) handle(ctx context.Context, c net.Conn) {
	_ = c.SetDeadline(time.Now().Add(35 * time.Second))
	var req struct {
		Op string `json:"op"`
	}
	if err := json.NewDecoder(io.LimitReader(c, 4096)).Decode(&req); err != nil {
		return
	}
	switch req.Op {
	case "subscribe":
		s.subscribe(ctx, c)
	case "status":
		if s.State != nil {
			_ = json.NewEncoder(c).Encode(s.State())
		}
	case "wake":
		if s.Wake != nil {
			s.Wake()
		}
		_ = json.NewEncoder(c).Encode(struct{}{})
	case "wait":
		if s.Unread == nil {
			return
		}
		// Subscribe before reading durable state: an arrival during Unread must
		// either appear in the query or close this captured channel.
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		yes, err := s.Unread(ctx)
		if err != nil {
			return
		}
		if !yes {
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return
			case <-changed:
			case <-timer.C:
			}
		}
		_ = json.NewEncoder(c).Encode(struct{}{})
	}
}

func Request(ctx context.Context, path, op string, dest any) error {
	c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer c.Close()
	deadline := time.Now().Add(3 * time.Second)
	if op == "wait" {
		deadline = time.Now().Add(33 * time.Second)
	}
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if err = json.NewEncoder(c).Encode(map[string]string{"op": op}); err == nil {
		err = json.NewDecoder(c).Decode(dest)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
