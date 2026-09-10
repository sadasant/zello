// Package channel implements the Zello Channel API voice transport. Callbacks
// run synchronously: they must return promptly and leave network work to workers.
package channel

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	maxAudioBytes    = 16 << 20
	maxAudioDuration = 10 * time.Minute
	maxStreams       = 8
	commandTimeout   = 15 * time.Second
)

var ErrDisconnected = errors.New("Zello connection closed")
var ErrNotConnected = errors.New("Zello channel is not connected")
var ErrBusy = errors.New("Zello channel is busy")

type Config struct{ Endpoint, Username, Password, Channel string }

type Incoming struct {
	StreamID        uint32
	Sender, Channel string
	Header          []byte
	PacketDuration  time.Duration
	Packets         [][]byte
	Err             error
}

type Transcript struct {
	StreamID uint32
	Text     string
	// Confidence is Zello's own, between 0 and 1. It is the only measure of
	// audio quality either end of this hotline has, and the question that
	// produced this field was whether a sentence that does not parse was
	// misheard or meant.
	Confidence float64
	Truncated  bool
}

// TextMessage is a message someone typed rather than spoke. Zello delivers it
// as a single event with no stream behind it, so there is no audio to save and
// nothing to transcribe: the text has already arrived.
type TextMessage struct {
	Sender, Channel, Text string
}

type Callbacks struct {
	Audio      func(Incoming)
	Transcript func(Transcript)
	Text       func(TextMessage)
	State      func(bool)
	// Shape reports an event's wire fields for diagnosis. Temporary.
	Shape func(command, fields string)
}

// SendError identifies an ambiguous transmission. Started is true if starting
// the stream may have succeeded, even if its response was lost. Callers must not
// automatically replay such a message: some or all of it may have been heard.
type SendError struct {
	Err     error
	Started bool
}

func (e *SendError) Error() string { return e.Err.Error() }
func (e *SendError) Unwrap() error { return e.Err }

type Client struct {
	cfg       Config
	cb        Callbacks
	mu        sync.Mutex
	session   *session
	running   bool
	connected atomic.Bool
	stateMu   sync.Mutex
	sendGate  chan struct{}
}

type session struct {
	conn               *websocket.Conn
	done               chan struct{}
	ready              chan struct{}
	writeMu            sync.Mutex
	mu                 sync.Mutex
	nextSeq            uint64
	pending            map[uint64]chan envelope
	authorized, online bool
}

type envelope struct {
	Command        string  `json:"command"`
	Seq            uint64  `json:"seq"`
	Success        bool    `json:"success"`
	Error          string  `json:"error"`
	Channel        string  `json:"channel"`
	Status         string  `json:"status"`
	Type           string  `json:"type"`
	Codec          string  `json:"codec"`
	CodecHeader    string  `json:"codec_header"`
	PacketDuration float64 `json:"packet_duration"`
	StreamID       uint32  `json:"stream_id"`
	// Zello names this field `stream_id` on the stream events and `streamId`
	// on `on_transcription`. Decoding only the first meant every transcript
	// arrived claiming stream 0, matched no message, and was discarded --
	// which is why native transcription looked switched off for as long as
	// this service has run. Observed 2026-09-10:
	//   command=on_transcription confidence=0.94 streamId=30319 truncated=false
	StreamIDCamel uint32  `json:"streamId"`
	Confidence    float64 `json:"confidence"`
	From          string  `json:"from"`
	Text          string  `json:"text"`
	Truncated     bool    `json:"truncated"`
}

type stream struct {
	Incoming
	bytes      int
	lastPacket uint32
	havePacket bool
}

func New(cfg Config, cb Callbacks) *Client {
	return &Client{cfg: cfg, cb: cb, sendGate: make(chan struct{}, 1)}
}
func (c *Client) Connected() bool { return c.connected.Load() }
func (c *Client) state(online bool) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.connected.Swap(online) != online && c.cb.State != nil {
		c.cb.State(online)
	}
}

func (c *Client) sessionState(s *session) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	s.mu.Lock()
	online := s.authorized && s.online
	s.mu.Unlock()
	if c.connected.Swap(online) != online && c.cb.State != nil {
		c.cb.State(online)
	}
}

// Run owns one WebSocket connection until cancellation or a connection error.
// Reconnection and backoff belong to the service. Only one Run may be active.
func (c *Client) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("Zello client is already running")
	}
	c.running = true
	c.mu.Unlock()
	defer func() { c.state(false); c.mu.Lock(); c.running = false; c.session = nil; c.mu.Unlock() }()
	dialer := websocket.Dialer{HandshakeTimeout: commandTimeout}
	conn, response, err := dialer.DialContext(ctx, c.cfg.Endpoint, nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("Zello WebSocket connection failed")
	}
	defer conn.Close()
	s := &session{conn: conn, done: make(chan struct{}), ready: make(chan struct{}, 1), pending: make(map[uint64]chan envelope)}
	c.mu.Lock()
	c.session = s
	c.mu.Unlock()
	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
	// The API sends ping every 30 seconds. Reply immediately while refreshing
	// the deadline; no timers or outgoing application commands precede logon.
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})
	readResult := make(chan error, 1)
	go func() { readResult <- c.read(s); close(s.done) }()
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-s.done:
		}
	}()
	loginCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	res, _, err := s.request(loginCtx, map[string]any{
		"command": "logon", "username": c.cfg.Username, "password": c.cfg.Password,
		"channels": []string{c.cfg.Channel}, "features": map[string]bool{"transcriptions": true},
	})
	cancel()
	if err == nil {
		err = responseError(res)
	}
	if err != nil {
		conn.Close()
		<-readResult
		return err
	}
	s.mu.Lock()
	s.authorized = true
	online := s.online
	s.mu.Unlock()
	c.sessionState(s)
	if !online {
		joined := time.NewTimer(commandTimeout)
		select {
		case <-s.ready:
			joined.Stop()
		case err := <-readResult:
			joined.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		case <-joined.C:
			conn.Close()
			<-readResult
			return errors.New("Zello channel did not become online")
		}
	}
	err = <-readResult
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Server errors are an allowlisted code, never arbitrary response content.
func responseError(e envelope) error {
	if e.Success && e.Error == "" {
		return nil
	}
	switch e.Error {
	case "channel is busy":
		return ErrBusy
	case "not authorized", "not enough params", "internal server error", "channels limit exceeded", "channel is not connected", "invalid codec", "invalid codec header", "listen only connection":
		return fmt.Errorf("Zello rejected command: %s", e.Error)
	default:
		return errors.New("Zello rejected command")
	}
}

func (s *session) write(ctx context.Context, kind int, data []byte) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.done:
		return false, ErrDisconnected
	default:
	}
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = s.conn.SetWriteDeadline(deadline)
	if err := s.conn.WriteMessage(kind, data); err != nil {
		s.conn.Close()
		return true, ErrDisconnected
	}
	return true, nil
}

func (s *session) request(ctx context.Context, fields map[string]any) (envelope, bool, error) {
	s.mu.Lock()
	s.nextSeq++
	seq := s.nextSeq
	response := make(chan envelope, 1)
	s.pending[seq] = response
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.pending, seq); s.mu.Unlock() }()
	fields["seq"] = seq
	data, err := json.Marshal(fields)
	if err != nil {
		return envelope{}, false, err
	}
	attempted, err := s.write(ctx, websocket.TextMessage, data)
	if err != nil {
		return envelope{}, attempted, err
	}
	select {
	case response := <-response:
		return response, attempted, nil
	case <-ctx.Done():
		return envelope{}, attempted, ctx.Err()
	case <-s.done:
		return envelope{}, attempted, ErrDisconnected
	}
}

// Send serializes voice messages and paces packets at their audio duration.
// A successful return means frames and stop_stream were written, not proof that
// any particular handset played them (the API provides no playback receipt).
func (c *Client) Send(ctx context.Context, header []byte, duration time.Duration, packets [][]byte) error {
	if err := validateAudio(header, duration); err != nil {
		return &SendError{Err: err}
	}
	if len(packets) == 0 || len(packets) > int(maxAudioDuration/duration) {
		return &SendError{Err: errors.New("voice message has invalid duration")}
	}
	bytes := 0
	for _, p := range packets {
		bytes += len(p)
		if len(p) == 0 || len(p) > 65536 || bytes > maxAudioBytes {
			return &SendError{Err: errors.New("voice message exceeds audio limits")}
		}
	}
	select {
	case c.sendGate <- struct{}{}:
		defer func() { <-c.sendGate }()
	case <-ctx.Done():
		return &SendError{Err: ctx.Err()}
	}
	c.mu.Lock()
	s := c.session
	c.mu.Unlock()
	if s == nil || !c.Connected() {
		return &SendError{Err: ErrNotConnected}
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	res, attempted, err := s.request(commandCtx, map[string]any{
		"command": "start_stream", "channel": c.cfg.Channel, "type": "audio", "codec": "opus",
		"codec_header": base64.StdEncoding.EncodeToString(header), "packet_duration": float64(duration) / float64(time.Millisecond),
	})
	cancel()
	if err != nil {
		return &SendError{Err: err, Started: attempted}
	}
	if err := responseError(res); err != nil {
		return &SendError{Err: err}
	}
	if res.StreamID == 0 {
		return &SendError{Err: errors.New("Zello returned an invalid stream ID"), Started: true}
	}
	stop := func(stopCtx context.Context) error {
		data, _ := json.Marshal(map[string]any{"command": "stop_stream", "stream_id": res.StreamID, "channel": c.cfg.Channel})
		_, err := s.write(stopCtx, websocket.TextMessage, data)
		return err
	}
	finished := false
	defer func() {
		if !finished {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = stop(cleanup)
		}
	}()
	for _, packet := range packets {
		if !c.Connected() {
			return &SendError{Err: ErrNotConnected, Started: true}
		}
		frame := make([]byte, 9+len(packet))
		frame[0] = 1
		binary.BigEndian.PutUint32(frame[1:5], res.StreamID)
		// Outgoing packet_id must be zero; the server ignores it.
		copy(frame[9:], packet)
		if _, err := s.write(ctx, websocket.BinaryMessage, frame); err != nil {
			return &SendError{Err: err, Started: true}
		}
		timer := time.NewTimer(duration)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return &SendError{Err: ctx.Err(), Started: true}
		case <-s.done:
			timer.Stop()
			return &SendError{Err: ErrDisconnected, Started: true}
		}
	}
	if err := stop(ctx); err != nil {
		return &SendError{Err: err, Started: true}
	}
	finished = true
	return nil
}

func validateAudio(header []byte, duration time.Duration) error {
	if len(header) != 4 || header[2] < 1 || header[2] > 2 || header[3] < 2 || header[3] > 60 || duration < 2500*time.Microsecond || duration > 60*time.Millisecond {
		return errors.New("unsupported Zello audio parameters")
	}
	switch binary.LittleEndian.Uint16(header[:2]) {
	case 8000, 12000, 16000, 24000, 48000:
		return nil
	}
	return errors.New("unsupported Zello sample rate")
}

func (c *Client) read(s *session) error {
	streams := make(map[uint32]*stream)
	complete := func(id uint32, err error) {
		st := streams[id]
		if st == nil {
			return
		}
		delete(streams, id)
		if st.Err == nil {
			st.Err = err
		}
		if c.cb.Audio != nil {
			c.cb.Audio(st.Incoming)
		}
	}
	defer func() {
		s.mu.Lock()
		s.authorized, s.online = false, false
		s.mu.Unlock()
		c.sessionState(s)
		for id := range streams {
			complete(id, ErrDisconnected)
		}
	}()
	for {
		kind, data, err := s.conn.ReadMessage()
		if err != nil {
			return ErrDisconnected
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		if kind == websocket.BinaryMessage {
			if len(data) < 9 || data[0] != 1 {
				continue
			}
			id := binary.BigEndian.Uint32(data[1:5])
			st := streams[id]
			if st == nil {
				continue
			}
			packetID := binary.BigEndian.Uint32(data[5:9])
			if st.havePacket && packetID <= st.lastPacket {
				continue
			}
			if st.havePacket && packetID != st.lastPacket+1 {
				st.Err = errors.New("incoming voice has missing packets")
			}
			st.lastPacket = packetID
			st.havePacket = true
			if len(data) == 9 || len(data)-9 > 65536 {
				complete(id, errors.New("invalid incoming audio packet"))
				continue
			}
			st.bytes += len(data) - 9
			if st.bytes > maxAudioBytes || time.Duration(len(st.Packets)+1)*st.PacketDuration > maxAudioDuration {
				complete(id, errors.New("incoming voice exceeds audio limits"))
				continue
			}
			st.Packets = append(st.Packets, append([]byte(nil), data[9:]...))
			continue
		}
		if kind != websocket.TextMessage {
			continue
		}
		var e envelope
		if err := json.Unmarshal(data, &e); err != nil {
			return errors.New("invalid Zello control message")
		}
		if e.Command == "" && e.Seq != 0 {
			s.mu.Lock()
			response := s.pending[e.Seq]
			s.mu.Unlock()
			if response != nil {
				select {
				case response <- e:
				default:
				}
			}
			continue
		}
		switch e.Command {
		case "on_channel_status":
			if e.Channel != c.cfg.Channel {
				continue
			}
			s.mu.Lock()
			s.online = e.Status == "online"
			if s.online {
				select {
				case s.ready <- struct{}{}:
				default:
				}
			}
			s.mu.Unlock()
			c.sessionState(s)
			if e.Status == "offline" {
				return errors.New("Zello channel is offline")
			}
		case "on_error":
			return errors.New("Zello server reported an error")
		case "on_transcription":
			// Temporary, 2026-09-10. The service matches a transcript to its
			// message by stream_id, and every transcript observed so far has
			// arrived with stream_id 0 while the audio stream it belongs to had
			// a real id -- so the lookup never matches and the transcript is
			// filed under a key nothing claims. Which field actually carries the
			// identifier is the question; this reports the envelope's shape,
			// with the transcript text itself replaced by its length so a
			// diagnostic does not copy what Daniel said into a second place.
			if c.cb.Shape != nil {
				c.cb.Shape("on_transcription", envelopeShape(data))
			}
			if c.cb.Transcript != nil {
				c.cb.Transcript(Transcript{StreamID: e.transcriptStream(), Text: e.Text,
					Confidence: e.Confidence, Truncated: e.Truncated})
			}
		case "on_text_message":
			// Typed messages arrived on the wire and were dropped in silence
			// until 2026-09-10: this switch handled five commands and let the
			// rest fall through without a word, so a message sent from a phone
			// looked delivered at one end and never existed at the other.
			c.deliverText(e)
		case "on_stream_start":
			if e.Type != "audio" || e.Channel != c.cfg.Channel {
				continue
			}
			if streams[e.StreamID] != nil {
				complete(e.StreamID, errors.New("incoming stream ID was reused"))
			}
			if len(streams) >= maxStreams {
				return errors.New("too many concurrent Zello streams")
			}
			header, err := base64.StdEncoding.DecodeString(e.CodecHeader)
			duration := time.Duration(e.PacketDuration * float64(time.Millisecond))
			if err == nil {
				err = validateAudio(header, duration)
			}
			if e.Codec != "opus" {
				err = errors.New("unsupported incoming Zello codec")
			}
			streams[e.StreamID] = &stream{Incoming: Incoming{StreamID: e.StreamID, Sender: e.From, Channel: e.Channel, Header: header, PacketDuration: duration}}
			if err != nil {
				complete(e.StreamID, err)
			}
		case "on_stream_stop":
			complete(e.StreamID, nil)
		}
	}
}

// deliverText hands a typed Zello message to the callback, if it belongs to this
// channel and carries anything. The read loop calls it and so do the tests --
// one implementation, because two copies of a rule are how the two answers
// start to differ.
func (c *Client) deliverText(e envelope) {
	if e.Channel != c.cfg.Channel {
		return
	}
	text := strings.TrimSpace(e.Text)
	if text == "" || c.cb.Text == nil {
		return
	}
	c.cb.Text(TextMessage{Sender: e.From, Channel: e.Channel, Text: text})
}

// envelopeShape logs only fixed diagnostic fields with their expected types.
// Unknown names and values (including nested translations) may contain message
// content, so none of them are rendered. Text itself is represented by byte count.
func envelopeShape(data []byte) string {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return "unparsable"
	}
	var parts []string
	for _, key := range []string{"stream_id", "streamId", "confidence", "truncated", "text"} {
		value, present := raw[key]
		if !present {
			continue
		}
		safe := "<invalid>"
		switch key {
		case "stream_id", "streamId":
			if n, ok := value.(float64); ok && n >= 0 && n <= 1<<32-1 && n == float64(uint32(n)) {
				safe = fmt.Sprintf("%d", uint32(n))
			}
		case "confidence":
			if n, ok := value.(float64); ok && n >= 0 && n <= 1 {
				safe = fmt.Sprintf("%g", n)
			}
		case "truncated":
			if b, ok := value.(bool); ok {
				safe = fmt.Sprintf("%t", b)
			}
		case "text":
			if text, ok := value.(string); ok {
				safe = fmt.Sprintf("<%d bytes>", len(text))
			}
		}
		parts = append(parts, key+"="+safe)
	}
	if len(parts) == 0 {
		return "<no diagnostic metadata>"
	}
	return strings.Join(parts, " ")
}

// transcriptStream returns the stream a transcript belongs to, from whichever
// spelling the server used. Preferring the snake_case field keeps the stream
// events authoritative if Zello ever sends both.
func (e envelope) transcriptStream() uint32 {
	if e.StreamID != 0 {
		return e.StreamID
	}
	return e.StreamIDCamel
}
