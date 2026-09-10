// Package service joins durable queues to voice transport without knowing their consumers.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

// nativeWatch is a temporary diagnostic, added 2026-09-10 at Daniel's request
// and meant to be removed once it has answered its question.
//
// Zello shows Daniel a transcript for every message he sends, and this service
// has never recorded receiving one: `native_transcription_observed` has been
// false for every voice message the floor has taken. Both facts cannot be the
// whole story, so something about the shape of `on_transcription` -- whether it
// arrives at all on this socket, how late, and whether it is flagged truncated
// -- is unknown, and the code was arranged so that it stayed unknown. The
// truncated branch below returned in silence.
//
// This changes no behaviour. It observes: every native transcript is logged the
// moment it arrives, whatever its shape, and a stream still waiting for one is
// reported every `nativeWatchEvery` until `nativeWatchFor` expires. It has its
// own lock because the ticker runs on its own goroutine, while the callbacks
// run on the client's read loop.
const (
	nativeWatchEvery = 30 * time.Second
	nativeWatchFor   = 15 * time.Minute
)

type nativeWait struct {
	id    string
	since time.Time
}

type nativeWatch struct {
	mu sync.Mutex
	// waiting holds incoming streams whose transcript has not arrived.
	waiting map[uint32]nativeWait
	// held is a transcript that arrived before any stream was waiting for it.
	// The next incoming stream claims it, if it appears soon enough -- an
	// identifier-less transcript can only be matched by position, so a stale one
	// must expire rather than attach itself to an unrelated message.
	held      string
	heldSince time.Time
}

const nativeHoldFor = 10 * time.Second

func newNativeWatch() *nativeWatch { return &nativeWatch{waiting: map[uint32]nativeWait{}} }

// add registers a stream as awaiting a transcript, and returns one already
// held if this stream is the only candidate for it.
func (w *nativeWatch) add(stream uint32, id string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.held != "" && len(w.waiting) == 0 && time.Since(w.heldSince) < nativeHoldFor {
		text := w.held
		w.held, w.heldSince = "", time.Time{}
		return text
	}
	w.held, w.heldSince = "", time.Time{}
	w.waiting[stream] = nativeWait{id: id, since: time.Now()}
	return ""
}

// hold keeps a transcript that arrived before its audio did.
func (w *nativeWatch) hold(text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.held, w.heldSince = text, time.Now()
}

// done reports how long the stream waited, and whether it was being watched at
// all -- a transcript for an unwatched stream is itself worth seeing, because it
// means one arrived before the audio did.
func (w *nativeWatch) done(stream uint32) (time.Duration, string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry, ok := w.waiting[stream]
	if !ok {
		return 0, "", false
	}
	delete(w.waiting, stream)
	return time.Since(entry.since), entry.id, true
}

// claim attributes a transcript that carries no stream id at all.
//
// Zello sends `streamId` only on transcripts of our own outgoing transmissions.
// A transcript of someone else's voice arrives with the field absent entirely
// -- observed 2026-09-10, `language=en-US` present and no identifier of any
// kind -- so there is nothing to match on and correlation has to be positional.
//
// The rule refuses to guess. Exactly one stream waiting is the ordinary case on
// a half-duplex channel and is attributed; more than one is ambiguous and is
// reported rather than assigned, because attaching the wrong words to the wrong
// message is worse than transcribing it ourselves.
func (w *nativeWatch) claim() (id string, waited time.Duration, waiting int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.waiting) != 1 {
		return "", 0, len(w.waiting)
	}
	for stream, entry := range w.waiting {
		delete(w.waiting, stream)
		return entry.id, time.Since(entry.since), 1
	}
	return "", 0, 0
}

// overdue returns the streams still waiting, and drops those past the window so
// a diagnostic cannot log forever.
func (w *nativeWatch) overdue() (still []string, gaveUp []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for stream, entry := range w.waiting {
		waited := time.Since(entry.since)
		line := fmt.Sprintf("%s (stream %d) after %s", entry.id, stream, waited.Round(time.Second))
		if waited > nativeWatchFor {
			delete(w.waiting, stream)
			gaveUp = append(gaveUp, line)
			continue
		}
		still = append(still, line)
	}
	sort.Strings(still)
	sort.Strings(gaveUp)
	return still, gaveUp
}

// watchNative reports what is still waiting until the connection ends.
func (s *Service) watchNative(ctx context.Context, done <-chan struct{}, w *nativeWatch) {
	ticker := time.NewTicker(nativeWatchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			still, gaveUp := w.overdue()
			for _, line := range gaveUp {
				s.log("native: no transcript for %s -- giving up after %s", line, nativeWatchFor)
			}
			for _, line := range still {
				s.log("native: still no transcript for %s", line)
			}
		}
	}
}

// Every connection owns its stream ID namespace, including early native events.
func (s *Service) connections(ctx context.Context, fail func(error)) error {
	delay := time.Second
	for ctx.Err() == nil {
		received := map[uint32]string{}
		early := map[uint32]string{}
		watch := newNativeWatch()
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
				if text := watch.add(in.StreamID, id); text != "" {
					s.log("native: transcript that arrived before its audio claimed by %s", id)
					s.complete(durable, id, text, true, fail)
				} else if text, ok := early[in.StreamID]; ok {
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
			Shape: func(command, fields string) {
				s.log("native: %s envelope: %s", command, fields)
			},
			Transcript: func(t channel.Transcript) {
				text := strings.TrimSpace(t.Text)
				if t.Truncated || text == "" {
					s.log("native: transcript for stream %d discarded (truncated=%v, empty=%v)",
						t.StreamID, t.Truncated, text == "")
					return
				}
				// A transcript carrying a stream id is one of our own outgoing
				// transmissions coming back; it belongs to no incoming message.
				// One with no id at all is a transcript of someone speaking to
				// us, and is matched by position.
				if t.StreamID != 0 {
					if id, ok := received[t.StreamID]; ok {
						s.log("native: transcript for %s (stream %d): %d chars, confidence=%.2f",
							id, t.StreamID, len(text), t.Confidence)
						s.complete(ctx, id, text, true, fail)
						delete(received, t.StreamID)
						watch.done(t.StreamID)
						return
					}
					// Either one of our own outgoing transmissions, which no
					// incoming message will ever claim, or a transcript that
					// overtook its audio. Held under its id: the first is never
					// claimed and ages out with the map, the second is claimed
					// the moment the audio lands.
					s.log("native: transcript for stream %d with no message yet: %d chars, confidence=%.2f",
						t.StreamID, len(text), t.Confidence)
					if len(early) >= 4096 {
						clear(early)
					}
					early[t.StreamID] = text
					return
				}
				id, waited, waiting := watch.claim()
				if waiting == 0 {
					// Its audio has not been stored yet. Held for the next one.
					s.log("native: transcript with no stream id arrived before any audio: holding (%d chars, confidence=%.2f)",
						len(text), t.Confidence)
					watch.hold(text)
					return
				}
				if waiting != 1 {
					s.log("native: transcript with no stream id and %d messages waiting: not attributed (%d chars, confidence=%.2f)",
						waiting, len(text), t.Confidence)
					return
				}
				s.log("native: transcript for %s after %s: %d chars, confidence=%.2f",
					id, waited.Round(time.Millisecond), len(text), t.Confidence)
				s.complete(ctx, id, text, true, fail)
			},
		}
		client := s.NewTransport(cb)
		s.mu.Lock()
		s.current = client
		s.mu.Unlock()
		began := time.Now()
		closed := make(chan struct{})
		go s.watchNative(ctx, closed, watch)
		err := client.Run(ctx)
		close(closed)
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
