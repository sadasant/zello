// Package service joins durable queues to voice transport without knowing their consumers.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sadasant/zello/internal/audio"
	"github.com/sadasant/zello/internal/channel"
	"github.com/sadasant/zello/internal/config"
	"github.com/sadasant/zello/internal/ipc"
	"github.com/sadasant/zello/internal/store"
)

type Speech interface {
	Transcribe(context.Context, string) (string, error)
	Synthesize(context.Context, string, string) error
}
type Transport interface {
	Run(context.Context) error
	Send(context.Context, []byte, time.Duration, [][]byte) error
	Connected() bool
}
type Service struct {
	Config       config.Config
	Paths        config.Paths
	Store        *store.Store
	Speech       Speech
	Logger       *log.Logger
	NewTransport func(channel.Callbacks) Transport
	NativeGrace  time.Duration
	mu           sync.Mutex
	current      Transport
	socket       *ipc.Server
	outgoing     chan struct{}
	incoming     chan struct{}
	native       atomic.Bool
}

func (s *Service) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func (s *Service) connection() Transport { s.mu.Lock(); defer s.mu.Unlock(); return s.current }
func (s *Service) connected() bool       { c := s.connection(); return c != nil && c.Connected() }
func (s *Service) status() ipc.Status {
	state := "disconnected"
	if s.connected() {
		state = "connected"
	}
	return ipc.Status{State: state, Running: true, NativeTranscription: s.native.Load()}
}
func (s *Service) log(format string, args ...any) {
	if s.Logger != nil {
		s.Logger.Printf(format, args...)
	}
}

func (s *Service) Run(parent context.Context) error {
	if err := s.Config.ValidateService(); err != nil {
		return err
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return errors.New("ffmpeg is required: brew install ffmpeg")
	}
	socket, err := ipc.Listen(s.Paths.Socket, s.Paths.Lock)
	if err != nil {
		return err
	}
	s.socket = socket
	defer socket.Close()
	if err = s.Store.Recover(parent); err != nil {
		return err
	}
	if s.NativeGrace == 0 {
		s.NativeGrace = 3 * time.Second
	}
	if s.NewTransport == nil {
		s.NewTransport = func(cb channel.Callbacks) Transport {
			return channel.New(channel.Config{Endpoint: s.Config.Endpoint(), Username: s.Config.Username, Password: s.Config.Password, Channel: s.Config.Channel}, cb)
		}
	}
	s.outgoing = make(chan struct{}, 1)
	s.incoming = make(chan struct{}, 1)
	socket.State = s.status
	socket.Wake = func() { s.signal(s.outgoing) }
	socket.Unread = func(ctx context.Context) (bool, error) { n, err := s.Store.Count(ctx); return n > 0, err }
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	faults := make(chan error, 1)
	fail := func(err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			select {
			case faults <- err:
			default:
			}
		}
	}
	var workers sync.WaitGroup
	start := func(work func(context.Context) error) {
		workers.Add(1)
		go func() { defer workers.Done(); fail(work(ctx)) }()
	}
	start(socket.Serve)
	start(func(ctx context.Context) error { return s.connections(ctx, fail) })
	start(s.transcriptions)
	start(s.sendQueue)
	s.log("service started")
	select {
	case <-parent.Done():
	case err = <-faults:
	}
	cancel()
	workers.Wait()
	s.log("service stopped")
	return err
}

// Every connection owns its stream ID namespace, including early native events.
func (s *Service) connections(ctx context.Context, fail func(error)) error {
	delay := time.Second
	for ctx.Err() == nil {
		received := map[uint32]string{}
		early := map[uint32]string{}
		cb := channel.Callbacks{
			State: func(online bool) {
				if online {
					s.log("Zello connected")
					s.signal(s.outgoing)
				} else {
					s.log("Zello disconnected")
				}
			},
			Audio: func(in channel.Incoming) {
				// Persist even if shutdown caused a partial-stream callback.
				durable, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				id, err := store.NewID()
				if err != nil {
					fail(errors.New("cannot allocate incoming message ID"))
					return
				}
				path := filepath.Join(s.Paths.Audio, id+".opus")
				saveErr := audio.SaveIncoming(path, in.Header, in.PacketDuration, in.Packets)
				if saveErr != nil {
					path = filepath.Join(s.Paths.Audio, id+".zello")
					if err = preserveRaw(path, in); err != nil {
						fail(errors.New("cannot preserve incoming audio"))
						return
					}
				}
				m := store.Message{ID: id, Sender: in.Sender, Channel: in.Channel, AudioPath: path, NextTranscriptionAt: time.Now().Add(s.NativeGrace)}
				if err = s.Store.SaveIncoming(durable, m); err != nil {
					fail(errors.New("cannot persist incoming message"))
					return
				}
				s.log("incoming %s saved", id)
				if saveErr != nil || in.Err != nil {
					reason := "incoming audio was incomplete; transcription withheld"
					if saveErr != nil {
						reason = "incoming audio retained as raw transport data: invalid or unsupported stream"
					}
					if err = s.Store.RejectIncoming(durable, id, reason); err != nil {
						fail(errors.New("cannot persist incoming audio failure"))
					}
					s.log("incoming %s: %s", id, reason)
					delete(early, in.StreamID)
					return
				}
				// Bound metadata retained while allowing delayed final native events.
				if len(received) >= 4096 {
					clear(received)
				}
				received[in.StreamID] = id
				if text, ok := early[in.StreamID]; ok {
					delete(early, in.StreamID)
					s.complete(durable, id, text, true, fail)
				}
				s.signal(s.incoming)
			},
			Text: func(tm channel.TextMessage) {
				// A typed message is already text. It skips the audio file, the
				// three-second wait for a native transcript and the OpenAI
				// fallback entirely, and is readable the moment it lands.
				durable, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				id, err := store.NewID()
				if err != nil {
					fail(errors.New("cannot allocate incoming message ID"))
					return
				}
				m := store.Message{ID: id, Sender: tm.Sender, Channel: tm.Channel}
				if err = s.Store.SaveIncoming(durable, m); err != nil {
					fail(errors.New("cannot persist incoming text message"))
					return
				}
				s.log("incoming %s saved (text)", id)
				// native=false: nothing was transcribed, so this must not be
				// evidence that native transcription works.
				s.complete(durable, id, tm.Text, false, fail)
				s.signal(s.incoming)
			},
			Transcript: func(t channel.Transcript) {
				text := strings.TrimSpace(t.Text)
				if t.Truncated || text == "" {
					return
				}
				if id, ok := received[t.StreamID]; ok {
					s.complete(ctx, id, text, true, fail)
					delete(received, t.StreamID)
				} else {
					if len(early) >= 4096 {
						clear(early)
					}
					early[t.StreamID] = text
				}
			},
		}
		client := s.NewTransport(cb)
		s.mu.Lock()
		s.current = client
		s.mu.Unlock()
		began := time.Now()
		err := client.Run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		// Transport errors contain no server response text or credentials.
		if err != nil {
			s.log("Zello reconnect pending: %v", err)
		}
		if time.Since(began) > time.Minute {
			delay = time.Second
		}
		if !pause(ctx, delay+time.Duration(rand.Int64N(int64(delay/4)+1))) {
			return nil
		}
		delay = min(delay*2, 30*time.Second)
	}
	return nil
}
func (s *Service) complete(ctx context.Context, id, text string, native bool, fail func(error)) {
	err := s.Store.CompleteIncoming(ctx, id, text)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		fail(errors.New("cannot persist incoming transcription"))
		return
	}
	if native {
		s.native.Store(true)
	}
	s.socket.Notify()
	s.log("incoming %s transcribed", id)
}
func (s *Service) transcriptions(ctx context.Context) error {
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	for ctx.Err() == nil {
		messages, err := s.Store.PendingIncoming(ctx)
		if err != nil {
			return err
		}
		for _, m := range messages {
			if ctx.Err() != nil {
				return nil
			}
			// Native transcription may have completed after the batch was read.
			latest, err := s.Store.Show(ctx, m.ID)
			if err != nil {
				return err
			}
			if latest.TranscriptionStatus != "pending" {
				continue
			}
			wav := filepath.Join(s.Paths.Audio, m.ID+".wav")
			err = audio.Normalize(ctx, m.AudioPath, wav)
			var text string
			if err == nil {
				text, err = s.Speech.Transcribe(ctx, wav)
			}
			_ = os.Remove(wav)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				if dbErr := s.Store.FailIncoming(ctx, m.ID, err.Error()); dbErr != nil && !errors.Is(dbErr, store.ErrNotFound) {
					return dbErr
				}
				s.log("incoming %s transcription deferred: %v", m.ID, err)
				continue
			}
			var completeErr error
			s.complete(ctx, m.ID, text, false, func(err error) { completeErr = err })
			if completeErr != nil {
				return completeErr
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.incoming:
		case <-timer.C:
		}
	}
	return nil
}
func (s *Service) sendQueue(ctx context.Context) error {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for ctx.Err() == nil {
		client := s.connection()
		if client != nil && client.Connected() {
			m, err := s.Store.ClaimOutgoing(ctx)
			if err == nil {
				retry, err := s.sendOne(ctx, client, m)
				if err != nil {
					return err
				}
				if retry && !pause(ctx, 2*time.Second) {
					return nil
				}
				continue
			}
			if !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.outgoing:
		case <-timer.C:
		}
	}
	return nil
}
func (s *Service) sendOne(ctx context.Context, client Transport, m store.Message) (bool, error) {
	path := m.AudioPath
	set := func(status, detail string) error {
		// Preserve retry/delivery state during orderly cancellation as well.
		durable, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Store.SetOutgoing(durable, m.ID, status, path, detail); err != nil {
			return err
		}
		s.log("outgoing %s %s", m.ID, status)
		return nil
	}
	if m.Channel != s.Config.Channel {
		return false, set("failed", "queued channel differs from configured channel")
	}
	if path == "" {
		path = filepath.Join(s.Paths.Audio, m.ID+"-out.mp3")
		if err := s.Speech.Synthesize(ctx, m.Text, path); err != nil {
			if ctx.Err() != nil {
				path = ""
				return false, set("queued", "")
			}
			return false, set("failed", err.Error())
		}
		if err := set("synthesizing", ""); err != nil {
			return false, err
		}
	}
	header, duration, packets, err := audio.Encode(ctx, path)
	if err != nil {
		if ctx.Err() != nil {
			return false, set("queued", "")
		}
		return false, set("failed", err.Error())
	}
	if err = set("sending", ""); err != nil {
		return false, err
	}
	err = client.Send(ctx, header, duration, packets)
	if err != nil {
		var sendErr *channel.SendError
		if errors.As(err, &sendErr) && !sendErr.Started {
			if errors.Is(err, channel.ErrNotConnected) || errors.Is(err, channel.ErrDisconnected) || errors.Is(err, channel.ErrBusy) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return true, set("queued", "")
			}
			return false, set("failed", err.Error())
		}
		return false, set("failed", "delivery status unknown after interrupted transmission; not retried to avoid duplicate speech")
	}
	return false, set("sent", "")
}
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Invalid or unsupported audio still has a durable original. This private capture
// is for diagnostics, never part of the text CLI or a transcription input.
func preserveRaw(path string, in channel.Incoming) error {
	f, err := os.CreateTemp(filepath.Dir(path), "capture-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	capture := struct {
		Header           []byte   `json:"header"`
		PacketDurationMS float64  `json:"packet_duration_ms"`
		Packets          [][]byte `json:"packets"`
	}{in.Header, float64(in.PacketDuration) / float64(time.Millisecond), in.Packets}
	err = json.NewEncoder(f).Encode(capture)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}
