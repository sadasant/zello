package channel

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var testHeader = []byte{0x80, 0x3e, 1, 20}

func testServer(t *testing.T, handler func(*websocket.Conn)) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		handler(conn)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func login(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	var command map[string]any
	if err := conn.ReadJSON(&command); err != nil {
		t.Error(err)
		return
	}
	if command["command"] != "logon" || command["username"] != "test-user" || command["password"] != "test-password" {
		t.Error("incorrect logon command")
	}
	if _, ok := command["auth_token"]; ok {
		t.Error("Work logon must not use a Friends and Family token")
	}
	features, ok := command["features"].(map[string]any)
	if !ok || features["transcriptions"] != true {
		t.Error("native transcription not requested")
	}
	if channels, ok := command["channels"].([]any); !ok || len(channels) != 1 || channels[0] != "test-channel" {
		t.Error("channel not joined at logon")
	}
	_ = conn.WriteJSON(map[string]any{"seq": command["seq"], "success": true})
	_ = conn.WriteJSON(map[string]any{"command": "on_channel_status", "channel": "test-channel", "status": "online"})
}

func startIncoming(conn *websocket.Conn, id uint32) {
	_ = conn.WriteJSON(map[string]any{"command": "on_stream_start", "type": "audio", "codec": "opus", "codec_header": base64.StdEncoding.EncodeToString(testHeader), "packet_duration": 20, "stream_id": id, "from": "human", "channel": "test-channel"})
}

func packet(conn *websocket.Conn, streamID, packetID uint32, payload []byte) {
	data := make([]byte, 9+len(payload))
	data[0] = 1
	binary.BigEndian.PutUint32(data[1:5], streamID)
	binary.BigEndian.PutUint32(data[5:9], packetID)
	copy(data[9:], payload)
	_ = conn.WriteMessage(websocket.BinaryMessage, data)
}

func newTestClient(t *testing.T, endpoint string, callbacks Callbacks) (*Client, context.CancelFunc, <-chan error) {
	t.Helper()
	client := New(Config{Endpoint: endpoint, Username: "test-user", Password: "test-password", Channel: "test-channel"}, callbacks)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()
	t.Cleanup(cancel)
	return client, cancel, result
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result")
		var zero T
		return zero
	}
}

func TestReceivesAudioAndNativeTranscriptions(t *testing.T) {
	got := make(chan Incoming, 1)
	transcripts := make(chan Transcript, 2)
	endpoint := testServer(t, func(conn *websocket.Conn) {
		login(t, conn)
		startIncoming(conn, 42)
		packet(conn, 42, 1, []byte{1, 2})
		packet(conn, 42, 2, []byte{3, 4})
		_ = conn.WriteJSON(map[string]any{"command": "on_transcription", "stream_id": 42, "text": "partial", "truncated": true})
		_ = conn.WriteJSON(map[string]any{"command": "on_stream_stop", "stream_id": 42})
		_ = conn.WriteJSON(map[string]any{"command": "on_transcription", "stream_id": 42, "text": "Can you hear me?", "truncated": false})
		_, _, _ = conn.ReadMessage()
	})
	_, cancel, result := newTestClient(t, endpoint, Callbacks{Audio: func(v Incoming) { got <- v }, Transcript: func(v Transcript) { transcripts <- v }})
	incoming := receive(t, got)
	if incoming.Err != nil || incoming.StreamID != 42 || incoming.Sender != "human" || incoming.Channel != "test-channel" || incoming.PacketDuration != 20*time.Millisecond || !bytes.Equal(incoming.Header, testHeader) {
		t.Fatalf("bad incoming metadata: %+v", incoming)
	}
	if len(incoming.Packets) != 2 || !bytes.Equal(incoming.Packets[1], []byte{3, 4}) {
		t.Fatalf("bad packets: %v", incoming.Packets)
	}
	if !receive(t, transcripts).Truncated {
		t.Error("lost truncated flag")
	}
	if final := receive(t, transcripts); final.Truncated || final.Text != "Can you hear me?" {
		t.Errorf("bad transcript: %+v", final)
	}
	cancel()
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Errorf("shutdown: %v", err)
	}
}

func TestSendsPacedAudioWithExactFramingAndPongs(t *testing.T) {
	online := make(chan bool, 2)
	observed := make(chan time.Duration, 1)
	endpoint := testServer(t, func(conn *websocket.Conn) {
		login(t, conn)
		var command map[string]any
		if err := conn.ReadJSON(&command); err != nil {
			t.Error(err)
			return
		}
		if command["command"] != "start_stream" || command["codec"] != "opus" || command["packet_duration"] != float64(20) || command["codec_header"] != base64.StdEncoding.EncodeToString(testHeader) {
			t.Errorf("incorrect start stream: %v", command)
		}
		_ = conn.WriteJSON(map[string]any{"seq": 9999, "success": false, "error": "unrelated response"})
		_ = conn.WriteJSON(map[string]any{"seq": command["seq"], "success": true, "stream_id": 77})
		pong := false
		conn.SetPongHandler(func(data string) error { pong = data == "keepalive"; return nil })
		_ = conn.WriteControl(websocket.PingMessage, []byte("keepalive"), time.Now().Add(time.Second))
		started := time.Now()
		for i := 0; i < 3; i++ {
			kind, data, err := conn.ReadMessage()
			if err != nil {
				t.Error(err)
				return
			}
			if kind != websocket.BinaryMessage || len(data) != 11 || data[0] != 1 || binary.BigEndian.Uint32(data[1:5]) != 77 || binary.BigEndian.Uint32(data[5:9]) != 0 || !bytes.Equal(data[9:], []byte{byte(i), 4}) {
				t.Errorf("incorrect audio frame: %v", data)
			}
		}
		if err := conn.ReadJSON(&command); err != nil {
			t.Error(err)
			return
		}
		if command["command"] != "stop_stream" || command["stream_id"] != float64(77) || command["channel"] != "test-channel" {
			t.Errorf("incorrect stream stop: %v", command)
		}
		if !pong {
			t.Error("client did not pong while sending")
		}
		observed <- time.Since(started)
		_, _, _ = conn.ReadMessage()
	})
	client, cancel, result := newTestClient(t, endpoint, Callbacks{State: func(v bool) { online <- v }})
	if !receive(t, online) {
		t.Fatal("not connected")
	}
	if err := client.Send(context.Background(), testHeader, 20*time.Millisecond, [][]byte{{0, 4}, {1, 4}, {2, 4}}); err != nil {
		t.Fatal(err)
	}
	if elapsed := receive(t, observed); elapsed < 50*time.Millisecond {
		t.Errorf("audio was sent without pacing: %v", elapsed)
	}
	cancel()
	receive(t, result)
	if client.Connected() {
		t.Error("still connected after stop")
	}
}

func TestPreservesPartialAudioOnDisconnectAndPacketGap(t *testing.T) {
	got := make(chan Incoming, 1)
	endpoint := testServer(t, func(conn *websocket.Conn) {
		login(t, conn)
		startIncoming(conn, 41)
		packet(conn, 41, 7, []byte{1})
		packet(conn, 41, 9, []byte{2})
	})
	_, _, result := newTestClient(t, endpoint, Callbacks{Audio: func(v Incoming) { got <- v }})
	incoming := receive(t, got)
	if incoming.Err == nil || len(incoming.Packets) != 2 {
		t.Fatalf("partial audio lost: %+v", incoming)
	}
	if receive(t, result) == nil {
		t.Error("disconnect did not fail Run")
	}
}

func TestServerErrorsCannotEchoCredentials(t *testing.T) {
	endpoint := testServer(t, func(conn *websocket.Conn) {
		var command map[string]any
		_ = conn.ReadJSON(&command)
		_ = conn.WriteJSON(map[string]any{"seq": command["seq"], "error": "password=test-password"})
	})
	_, _, result := newTestClient(t, endpoint, Callbacks{})
	err := receive(t, result)
	if err == nil || strings.Contains(err.Error(), "test-password") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestSendUncertaintyAndKnownRejection(t *testing.T) {
	for _, fail := range []string{"rejected", "after-start", "after-audio"} {
		t.Run(fail, func(t *testing.T) {
			online := make(chan bool, 2)
			endpoint := testServer(t, func(conn *websocket.Conn) {
				login(t, conn)
				var command map[string]any
				if err := conn.ReadJSON(&command); err != nil {
					t.Error(err)
					return
				}
				if fail == "rejected" {
					_ = conn.WriteJSON(map[string]any{"seq": command["seq"], "error": "channel is busy"})
					_, _, _ = conn.ReadMessage()
					return
				}
				if fail == "after-start" {
					return
				}
				_ = conn.WriteJSON(map[string]any{"seq": command["seq"], "success": true, "stream_id": 1})
				_, _, _ = conn.ReadMessage()
			})
			client, cancel, result := newTestClient(t, endpoint, Callbacks{State: func(v bool) { online <- v }})
			receive(t, online)
			err := client.Send(context.Background(), testHeader, 20*time.Millisecond, [][]byte{{1}, {2}})
			var sendErr *SendError
			if !errors.As(err, &sendErr) || sendErr.Started != (fail != "rejected") {
				t.Fatalf("wrong send uncertainty: %v", err)
			}
			if fail == "rejected" && !errors.Is(err, ErrBusy) {
				t.Fatalf("channel contention did not preserve retry classification: %v", err)
			}
			cancel()
			receive(t, result)
		})
	}
}

func TestWaitsForChannelOnlineBeforeSend(t *testing.T) {
	loggedIn := make(chan struct{})
	release := make(chan struct{})
	online := make(chan bool, 2)
	endpoint := testServer(t, func(conn *websocket.Conn) {
		var command map[string]any
		_ = conn.ReadJSON(&command)
		_ = conn.WriteJSON(map[string]any{"seq": command["seq"], "success": true})
		close(loggedIn)
		<-release
		_ = conn.WriteJSON(map[string]any{"command": "on_channel_status", "channel": "other", "status": "online"})
		_ = conn.WriteJSON(map[string]any{"command": "on_channel_status", "channel": "test-channel", "status": "online"})
		_, _, _ = conn.ReadMessage()
	})
	client, cancel, result := newTestClient(t, endpoint, Callbacks{State: func(v bool) { online <- v }})
	receive(t, loggedIn)
	if client.Connected() {
		t.Error("connected before channel status")
	}
	if err := client.Send(context.Background(), testHeader, 20*time.Millisecond, [][]byte{{1}}); !errors.Is(err, ErrNotConnected) {
		t.Errorf("send before channel online: %v", err)
	}
	close(release)
	if !receive(t, online) {
		t.Error("channel did not become online")
	}
	cancel()
	receive(t, result)
}

func TestIncomingLimitPreservesPrefix(t *testing.T) {
	got := make(chan Incoming, 1)
	endpoint := testServer(t, func(conn *websocket.Conn) {
		login(t, conn)
		startIncoming(conn, 2)
		payload := bytes.Repeat([]byte{1}, 65536)
		for i := uint32(0); i <= maxAudioBytes/65536; i++ {
			packet(conn, 2, i, payload)
		}
		_, _, _ = conn.ReadMessage()
	})
	_, cancel, result := newTestClient(t, endpoint, Callbacks{Audio: func(v Incoming) { got <- v }})
	incoming := receive(t, got)
	if incoming.Err == nil || len(incoming.Packets) != maxAudioBytes/65536 {
		t.Errorf("wrong bounded prefix: packets=%d err=%v", len(incoming.Packets), incoming.Err)
	}
	cancel()
	receive(t, result)
}

func TestMalformedControlFailsWithoutContentLeak(t *testing.T) {
	endpoint := testServer(t, func(conn *websocket.Conn) {
		login(t, conn)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"password":"test-password"`))
	})
	_, _, result := newTestClient(t, endpoint, Callbacks{})
	err := receive(t, result)
	if err == nil || strings.Contains(err.Error(), "test-password") {
		t.Fatalf("bad parse error: %v", err)
	}
}

func TestEnvelopeAllowsFractionalPacketDuration(t *testing.T) {
	var e envelope
	if err := json.Unmarshal([]byte(`{"packet_duration":2.5}`), &e); err != nil || e.PacketDuration != 2.5 {
		t.Fatal("fractional Opus duration rejected")
	}
}

func TestClientCanRunAgainAfterDisconnect(t *testing.T) {
	online := make(chan bool, 4)
	release := make(chan struct{}, 2)
	endpoint := testServer(t, func(conn *websocket.Conn) {
		login(t, conn)
		<-release
	})
	client := New(Config{Endpoint: endpoint, Username: "test-user", Password: "test-password", Channel: "test-channel"}, Callbacks{State: func(v bool) { online <- v }})
	for i := 0; i < 2; i++ {
		result := make(chan error, 1)
		go func() { result <- client.Run(context.Background()) }()
		if !receive(t, online) || !client.Connected() {
			t.Fatal("reconnection did not become online")
		}
		release <- struct{}{}
		if receive(t, result) == nil {
			t.Fatal("disconnect should return an error for service backoff")
		}
		if receive(t, online) || client.Connected() {
			t.Fatal("disconnect did not reset state")
		}
	}
}

// A typed Zello message must reach the callback. These arrived on the wire and
// were dropped in silence until 2026-09-10, because the switch handled five
// commands and let the rest fall through without a word.
func TestOnTextMessageReachesTheCallback(t *testing.T) {
	cases := []struct {
		name, channel, from, text string
		configured                string
		want                      string
	}{
		{name: "delivered", channel: "agents", from: "admin", text: "hello there", configured: "agents", want: "hello there"},
		{name: "trimmed", channel: "agents", from: "admin", text: "  spaced  ", configured: "agents", want: "spaced"},
		{name: "other channel ignored", channel: "elsewhere", from: "admin", text: "not mine", configured: "agents", want: ""},
		{name: "empty ignored", channel: "agents", from: "admin", text: "   ", configured: "agents", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got TextMessage
			c := &Client{cfg: Config{Channel: tc.configured}, cb: Callbacks{
				Text: func(m TextMessage) { got = m },
			}}
			e := envelope{Command: "on_text_message", Channel: tc.channel, From: tc.from, Text: tc.text}
			c.deliverText(e)
			if got.Text != tc.want {
				t.Fatalf("text = %q, want %q", got.Text, tc.want)
			}
			if tc.want != "" && got.Sender != tc.from {
				t.Fatalf("sender = %q, want %q", got.Sender, tc.from)
			}
		})
	}
}

// A nil callback must not panic: the transport is shared, and a caller that
// does not want text messages should simply not receive them.
func TestOnTextMessageWithNoCallbackIsSafe(t *testing.T) {
	c := &Client{cfg: Config{Channel: "agents"}}
	c.deliverText(envelope{Command: "on_text_message", Channel: "agents", Text: "hi"})
}

// Zello spells the field `streamId` on on_transcription and `stream_id` on the
// stream events. Decoding only the second meant every transcript claimed stream
// 0, matched no message, and was silently discarded.
func TestTranscriptStreamAcceptsEitherSpelling(t *testing.T) {
	for _, c := range []struct {
		name string
		raw  string
		want uint32
	}{
		{"camelCase, as on_transcription sends it", `{"command":"on_transcription","streamId":30319}`, 30319},
		{"snake_case stays authoritative", `{"command":"on_transcription","stream_id":22370}`, 22370},
		{"snake_case wins when both are present", `{"command":"on_transcription","stream_id":1,"streamId":2}`, 1},
		{"neither present is still zero", `{"command":"on_transcription"}`, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			var e envelope
			if err := json.Unmarshal([]byte(c.raw), &e); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := e.transcriptStream(); got != c.want {
				t.Fatalf("stream id: got %d, want %d", got, c.want)
			}
		})
	}
}

func TestTranscriptCarriesConfidence(t *testing.T) {
	var e envelope
	if err := json.Unmarshal([]byte(`{"command":"on_transcription","streamId":7,"confidence":0.9456}`), &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if e.Confidence < 0.94 || e.Confidence > 0.95 {
		t.Fatalf("confidence not decoded: %v", e.Confidence)
	}
}

func TestEnvelopeShapeKeepsOnlyUsefulTypedMetadata(t *testing.T) {
	input := `{"command":"on_transcription","stream_id":12,"streamId":34,"confidence":0.9456,"truncated":false,"text":"hello"}`
	want := "stream_id=12 streamId=34 confidence=0.9456 truncated=false text=<5 bytes>"
	if got := envelopeShape([]byte(input)); got != want {
		t.Fatalf("diagnostic metadata=%q, want %q", got, want)
	}
}

func TestEnvelopeShapeOmitsNestedAndUnexpectedContent(t *testing.T) {
	input := `{
		"command":"on_transcription",
		"text":"private speech",
		"translations":[{"language":"es","message":"private translated speech"}],
		"sender":"private sender",
		"short":"secret",
		"extra":{"text":"nested secret","values":["another secret"]},
		"private field\ninjected log line":"do not render the field name either"
	}`
	if got := envelopeShape([]byte(input)); got != "text=<14 bytes>" {
		t.Fatalf("diagnostic exposed more than text length: %q", got)
	}
	if got := envelopeShape([]byte(`{"private field\ninjected":"secret","translations":[{"message":"private speech"}]}`)); got != "<no diagnostic metadata>" {
		t.Fatalf("unknown fields were rendered: %q", got)
	}
}

func TestEnvelopeShapeRejectsUnexpectedTypesAndRanges(t *testing.T) {
	cases := []struct{ input, want string }{
		{`{"stream_id":"secret","streamId":{"message":"nested secret"},"confidence":["private"],"truncated":"secret","text":{"message":"private speech"}}`,
			"stream_id=<invalid> streamId=<invalid> confidence=<invalid> truncated=<invalid> text=<invalid>"},
		{`{"stream_id":-1,"streamId":4294967296,"confidence":2,"truncated":null,"text":null}`,
			"stream_id=<invalid> streamId=<invalid> confidence=<invalid> truncated=<invalid> text=<invalid>"},
		{`{"stream_id":1.5,"streamId":true,"confidence":-0.1,"truncated":1,"text":["private speech"]}`,
			"stream_id=<invalid> streamId=<invalid> confidence=<invalid> truncated=<invalid> text=<invalid>"},
		{`{"stream_id":null,"confidence":"0.94"}`, "stream_id=<invalid> confidence=<invalid>"},
		{`{"stream_id":0,"streamId":4294967295,"confidence":1,"truncated":true,"text":""}`,
			"stream_id=0 streamId=4294967295 confidence=1 truncated=true text=<0 bytes>"},
		{`{"text":"private speech"`, "unparsable"},
	}
	for _, tc := range cases {
		if got := envelopeShape([]byte(tc.input)); got != tc.want {
			t.Errorf("diagnostic=%q, want %q", got, tc.want)
		}
	}
}
