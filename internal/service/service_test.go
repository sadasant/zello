package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sadasant/zello/internal/audio"
	"github.com/sadasant/zello/internal/channel"
	"github.com/sadasant/zello/internal/config"
	"github.com/sadasant/zello/internal/ipc"
	"github.com/sadasant/zello/internal/store"
)

// These typed fakes replace remote systems only. Tests use the production queue,
// service loops, Unix socket, and actual ffmpeg encoding/normalization.
type speechFake struct {
	wav            []byte
	transcriptions atomic.Int32
	syntheses      atomic.Int32
	transcribe     func(context.Context, string) (string, error)
	synthesize     func(context.Context, string, string) error
}

func (f *speechFake) Transcribe(ctx context.Context, path string) (string, error) {
	f.transcriptions.Add(1)
	data, err := os.ReadFile(path)
	if err != nil || len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return "", errors.New("expected normalized WAV")
	}
	if f.transcribe != nil {
		return f.transcribe(ctx, path)
	}
	return "fallback transcription", nil
}
func (f *speechFake) Synthesize(ctx context.Context, text, path string) error {
	f.syntheses.Add(1)
	if f.synthesize != nil {
		return f.synthesize(ctx, text, path)
	}
	return os.WriteFile(path, f.wav, 0600)
}

type transportEvent struct {
	audio      *channel.Incoming
	transcript *channel.Transcript
}
type transportFake struct {
	cb      channel.Callbacks
	online  atomic.Bool
	ready   chan struct{}
	events  chan transportEvent
	sent    chan channel.Incoming
	send    func(context.Context) error
	failure <-chan error
}

func (f *transportFake) Connected() bool { return f.online.Load() }
func (f *transportFake) Run(ctx context.Context) error {
	f.online.Store(true)
	f.cb.State(true)
	close(f.ready)
	defer func() { f.online.Store(false); f.cb.State(false) }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-f.failure:
			return err
		case e := <-f.events:
			if e.audio != nil {
				f.cb.Audio(*e.audio)
			}
			if e.transcript != nil {
				f.cb.Transcript(*e.transcript)
			}
		}
	}
}
func (f *transportFake) Send(ctx context.Context, header []byte, duration time.Duration, packets [][]byte) error {
	if f.send != nil {
		return f.send(ctx)
	}
	f.sent <- channel.Incoming{Header: header, PacketDuration: duration, Packets: packets}
	return nil
}

type fixture struct {
	paths  config.Paths
	db     *store.Store
	speech *speechFake
	voice  channel.Incoming
}

func makeFixture(t *testing.T) *fixture {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	// Keep Unix socket paths below macOS's sockaddr_un limit.
	dir, err := os.MkdirTemp("/tmp", "zello-service-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	p := config.PathsAt(dir)
	if err = config.Prepare(p); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(p.DB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	wav := make([]byte, 44+3200*2)
	copy(wav, "RIFF")
	binary.LittleEndian.PutUint32(wav[4:], uint32(len(wav)-8))
	copy(wav[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(wav[16:], 16)
	binary.LittleEndian.PutUint16(wav[20:], 1)
	binary.LittleEndian.PutUint16(wav[22:], 1)
	binary.LittleEndian.PutUint32(wav[24:], 16000)
	binary.LittleEndian.PutUint32(wav[28:], 32000)
	binary.LittleEndian.PutUint16(wav[32:], 2)
	binary.LittleEndian.PutUint16(wav[34:], 16)
	copy(wav[36:], "data")
	binary.LittleEndian.PutUint32(wav[40:], uint32(len(wav)-44))
	for i := 44; i < len(wav); i += 2 {
		binary.LittleEndian.PutUint16(wav[i:], uint16((i%80)*200))
	}
	path := filepath.Join(dir, "fixture.wav")
	if err = os.WriteFile(path, wav, 0600); err != nil {
		t.Fatal(err)
	}
	header, duration, packets, err := audio.Encode(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{paths: p, db: db, speech: &speechFake{wav: wav}, voice: channel.Incoming{StreamID: 42, Sender: "human", Channel: "test-channel", Header: header, PacketDuration: duration, Packets: packets}}
}

func startFixture(t *testing.T, f *fixture, grace time.Duration, send func(context.Context) error) (*Service, *transportFake, func()) {
	t.Helper()
	transport := &transportFake{ready: make(chan struct{}), events: make(chan transportEvent, 8), sent: make(chan channel.Incoming, 8), send: send}
	s := &Service{Config: config.Config{Network: "test", Username: "service", Password: "fixture-password", Channel: "test-channel"}, Paths: f.paths, Store: f.db, Speech: f.speech, NativeGrace: grace, NewTransport: func(cb channel.Callbacks) Transport { transport.cb = cb; return transport }}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	select {
	case <-transport.ready:
	case err := <-result:
		t.Fatalf("service failed to start: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("service startup timed out")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("service shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("service shutdown timed out")
		}
	}
	t.Cleanup(stop)
	return s, transport, stop
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected state was not reached")
}
func unread(t *testing.T, f *fixture, text string) store.Message {
	t.Helper()
	var result store.Message
	eventually(t, func() bool {
		m, err := f.db.Peek(context.Background())
		if err == nil {
			result = m
			return true
		}
		return false
	})
	if result.Text != text {
		t.Fatalf("transcription=%q, expected %q", result.Text, text)
	}
	if info, err := os.Stat(result.AudioPath); err != nil || info.Size() == 0 {
		t.Fatal("original incoming audio was not preserved")
	}
	return result
}

func TestNativeTranscriptionBeforeAndAfterAudio(t *testing.T) {
	for _, order := range []string{"before", "after"} {
		t.Run(order, func(t *testing.T) {
			f := makeFixture(t)
			s, tr, _ := startFixture(t, f, 2*time.Second, nil)
			waitDone := make(chan error, 1)
			go func() {
				var response struct{}
				waitDone <- ipc.Request(context.Background(), f.paths.Socket, "wait", &response)
			}()
			select {
			case err := <-waitDone:
				t.Fatalf("wait returned before incoming message: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			transcript := channel.Transcript{StreamID: 42, Text: "Can you hear me?"}
			if order == "before" {
				tr.events <- transportEvent{transcript: &transcript}
			}
			tr.events <- transportEvent{audio: &f.voice}
			if order == "after" {
				tr.events <- transportEvent{transcript: &transcript}
			}
			m := unread(t, f, "Can you hear me?")
			select {
			case err := <-waitDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("native completion did not notify wait")
			}
			if !s.native.Load() || f.speech.transcriptions.Load() != 0 {
				t.Fatal("native path unnecessarily called fallback")
			}
			consumed, err := f.db.Next(context.Background())
			if err != nil || consumed.ID != m.ID {
				t.Fatal("could not consume native message")
			}
			if n, _ := f.db.Count(context.Background()); n != 0 {
				t.Fatal("consumed message remains unread")
			}
		})
	}
}

func TestTruncatedNativeUsesFullAudioFallback(t *testing.T) {
	f := makeFixture(t)
	s, tr, _ := startFixture(t, f, 10*time.Millisecond, nil)
	tr.events <- transportEvent{transcript: &channel.Transcript{StreamID: 42, Text: "incomplete text", Truncated: true}}
	tr.events <- transportEvent{audio: &f.voice}
	unread(t, f, "fallback transcription")
	if s.native.Load() || f.speech.transcriptions.Load() != 1 {
		t.Fatal("truncated native text was trusted or fallback was duplicated")
	}
}

func TestLateNativeWinsDuringFallbackWithoutOverwritingConsumedText(t *testing.T) {
	f := makeFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	f.speech.transcribe = func(ctx context.Context, path string) (string, error) {
		close(started)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "late fallback", nil
		}
	}
	_, tr, _ := startFixture(t, f, 10*time.Millisecond, nil)
	tr.events <- transportEvent{audio: &f.voice}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("fallback did not start")
	}
	tr.events <- transportEvent{transcript: &channel.Transcript{StreamID: 42, Text: "native final"}}
	m := unread(t, f, "native final")
	if err := f.db.Consume(context.Background(), m.ID); err != nil {
		t.Fatal(err)
	}
	close(release)
	eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(f.paths.Audio, m.ID+".wav"))
		return errors.Is(err, os.ErrNotExist)
	})
	if latest, err := f.db.Show(context.Background(), m.ID); err != nil || latest.Text != "native final" || latest.Status != "consumed" {
		t.Fatalf("late fallback mutated delivered text: %+v %v", latest, err)
	}
}

func TestRestartPreservesUnreadAndQueuedMessagesWithoutReplayingUncertainSend(t *testing.T) {
	f := makeFixture(t)
	ctx := context.Background()
	queued, err := f.db.Enqueue(ctx, "Testing one two three.", "test-channel")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.ClaimOutgoing(ctx); err != nil {
		t.Fatal(err)
	} // Interrupted TTS is safe to resume.
	uncertain, err := f.db.Enqueue(ctx, "Possibly already heard.", "test-channel")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.db.SetOutgoing(ctx, uncertain.ID, "sending", "", ""); err != nil {
		t.Fatal(err)
	}
	_, tr, stop := startFixture(t, f, time.Second, nil)
	eventually(t, func() bool { m, _ := f.db.Show(ctx, queued.ID); return m.Status == "sent" })
	select {
	case sent := <-tr.sent:
		if len(sent.Packets) == 0 || sent.PacketDuration != 20*time.Millisecond {
			t.Fatal("TTS did not produce transport audio")
		}
	default:
		t.Fatal("queued message never reached transport")
	}
	if m, _ := f.db.Show(ctx, uncertain.ID); m.Status != "failed" || m.Error == "" {
		t.Fatal("uncertain transmission was replayed")
	}
	tr.events <- transportEvent{audio: &f.voice, transcript: &channel.Transcript{StreamID: 42, Text: "durable incoming"}}
	incoming := unread(t, f, "durable incoming")
	stop()
	if err = f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db, err = store.Open(f.paths.DB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.db.Close() })
	second, err := f.db.Enqueue(ctx, "Queued while stopped.", "test-channel")
	if err != nil {
		t.Fatal(err)
	}
	_, tr2, _ := startFixture(t, f, time.Second, nil)
	eventually(t, func() bool { m, _ := f.db.Show(ctx, second.ID); return m.Status == "sent" })
	if m := unread(t, f, "durable incoming"); m.ID != incoming.ID {
		t.Fatal("restart changed unread identity")
	}
	if len(tr2.sent) != 1 {
		t.Fatal("restart replayed an already sent or uncertain message")
	}
}

func TestCancellationDuringSynthesisKeepsQueueAndDuringSendDoesNotReplay(t *testing.T) {
	for _, phase := range []string{"synthesis", "send"} {
		t.Run(phase, func(t *testing.T) {
			f := makeFixture(t)
			started := make(chan struct{})
			var send func(context.Context) error
			if phase == "synthesis" {
				f.speech.synthesize = func(ctx context.Context, text, path string) error { close(started); <-ctx.Done(); return ctx.Err() }
			} else {
				send = func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return &channel.SendError{Err: ctx.Err(), Started: true}
				}
			}
			m, err := f.db.Enqueue(context.Background(), "cancel test", "test-channel")
			if err != nil {
				t.Fatal(err)
			}
			_, _, stop := startFixture(t, f, time.Second, send)
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("outgoing phase did not start")
			}
			stop()
			latest, err := f.db.Show(context.Background(), m.ID)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "synthesis" && (latest.Status != "queued" || latest.AudioPath != "") {
				t.Fatalf("cancelled synthesis lost queue: %+v", latest)
			}
			if phase == "send" && (latest.Status != "failed" || latest.Error == "") {
				t.Fatalf("interrupted send incorrectly replayable: %+v", latest)
			}
		})
	}
}

func TestPendingTranscriptionSurvivesServiceRestart(t *testing.T) {
	f := makeFixture(t)
	started := make(chan struct{})
	f.speech.transcribe = func(ctx context.Context, path string) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	}
	_, tr, stop := startFixture(t, f, 10*time.Millisecond, nil)
	tr.events <- transportEvent{audio: &f.voice}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("transcription did not start")
	}
	stop()
	if n, _ := f.db.Count(context.Background()); n != 0 {
		t.Fatal("unfinished transcript leaked into inbox")
	}
	f.speech = &speechFake{wav: f.speech.wav}
	_, _, _ = startFixture(t, f, 10*time.Millisecond, nil)
	unread(t, f, "fallback transcription")
}

func TestInvalidIncomingPreservesOriginalWithoutOfferingPartialText(t *testing.T) {
	f := makeFixture(t)
	_, tr, _ := startFixture(t, f, 10*time.Millisecond, nil)
	invalid := f.voice
	invalid.Header = []byte{1, 2, 3}
	tr.events <- transportEvent{audio: &invalid}
	var path string
	eventually(t, func() bool {
		files, _ := filepath.Glob(filepath.Join(f.paths.Audio, "*.zello"))
		if len(files) != 1 {
			return false
		}
		path = files[0]
		id := strings.TrimSuffix(filepath.Base(path), ".zello")
		m, err := f.db.Show(context.Background(), id)
		return err == nil && m.TranscriptionStatus == "failed" && m.Error != "" && m.AudioPath == path
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var original struct {
		Header  []byte
		Packets [][]byte
	}
	if err = json.Unmarshal(data, &original); err != nil || len(original.Packets) != len(invalid.Packets) || len(original.Header) != 3 {
		t.Fatal("invalid message's original transport bytes were lost")
	}
	if n, _ := f.db.Count(context.Background()); n != 0 || f.speech.transcriptions.Load() != 0 {
		t.Fatal("unusable incoming audio was exposed or transcribed")
	}
}

func TestOnlySafeTransientSendFailuresAreQueuedAgain(t *testing.T) {
	for _, kind := range []string{"busy", "permanent", "uncertain"} {
		t.Run(kind, func(t *testing.T) {
			f := makeFixture(t)
			var sendErr error
			switch kind {
			case "busy":
				sendErr = &channel.SendError{Err: channel.ErrBusy}
			case "permanent":
				sendErr = &channel.SendError{Err: errors.New("Zello rejected command")}
			case "uncertain":
				sendErr = &channel.SendError{Err: channel.ErrDisconnected, Started: true}
			}
			client := &transportFake{send: func(context.Context) error { return sendErr }}
			s := &Service{Config: config.Config{Channel: "test-channel"}, Paths: f.paths, Store: f.db, Speech: f.speech}
			m, err := f.db.Enqueue(context.Background(), "failure classification", "test-channel")
			if err != nil {
				t.Fatal(err)
			}
			retry, err := s.sendOne(context.Background(), client, m)
			if err != nil || retry != (kind == "busy") {
				t.Fatalf("retry=%v err=%v", retry, err)
			}
			latest, err := f.db.Show(context.Background(), m.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := "failed"
			if kind == "busy" {
				want = "queued"
			}
			if latest.Status != want {
				t.Fatalf("status=%s want=%s", latest.Status, want)
			}
		})
	}
}

func TestAutomaticReconnectResumesQueuedMessage(t *testing.T) {
	f := makeFixture(t)
	m, err := f.db.Enqueue(context.Background(), "Deliver after reconnect.", "test-channel")
	if err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 1)
	firstAttempt := make(chan time.Time, 1)
	secondConnection := make(chan time.Time, 1)
	first := &transportFake{ready: make(chan struct{}), events: make(chan transportEvent), sent: make(chan channel.Incoming, 1), failure: failures}
	first.send = func(context.Context) error {
		firstAttempt <- time.Now()
		failures <- channel.ErrDisconnected
		return &channel.SendError{Err: channel.ErrDisconnected}
	}
	second := &transportFake{ready: make(chan struct{}), events: make(chan transportEvent), sent: make(chan channel.Incoming, 1)}
	var connections atomic.Int32
	s := &Service{Config: config.Config{Network: "test", Username: "service", Password: "fixture-password", Channel: "test-channel"}, Paths: f.paths, Store: f.db, Speech: f.speech,
		NewTransport: func(cb channel.Callbacks) Transport {
			if connections.Add(1) == 1 {
				first.cb = cb
				return first
			}
			second.cb = cb
			secondConnection <- time.Now()
			return second
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("reconnected service shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("reconnected service did not stop")
		}
	})
	var failedAt time.Time
	select {
	case failedAt = <-firstAttempt:
	case <-time.After(5 * time.Second):
		t.Fatal("first send did not encounter connection failure")
	}
	select {
	case reconnectedAt := <-secondConnection:
		if elapsed := reconnectedAt.Sub(failedAt); elapsed < 900*time.Millisecond || elapsed > 4*time.Second {
			t.Fatalf("unexpected reconnect backoff: %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("service did not reconnect automatically")
	}
	eventually(t, func() bool { latest, _ := f.db.Show(context.Background(), m.ID); return latest.Status == "sent" })
	if connections.Load() != 2 || len(first.sent) != 0 || len(second.sent) != 1 {
		t.Fatal("queued message did not move exactly once to the new connection")
	}
	if f.speech.syntheses.Load() != 1 {
		t.Fatal("reconnect unnecessarily synthesized the queued message again")
	}
}
