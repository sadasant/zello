package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sadasant/zello/internal/config"
)

func testClient() *Client {
	return New(config.Config{OpenAIKey: "fake-transcription-key", TranscribeModel: "gpt-4o-mini-transcribe", ElevenLabsKey: "fake-speech-key", VoiceID: "voice-example", SpeechModel: "eleven_flash_v2_5"})
}
func inputFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "private-original-name.wav")
	if err := os.WriteFile(p, []byte("synthetic audio fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}
func outputFile(t *testing.T) string { t.Helper(); return filepath.Join(t.TempDir(), "speech.mp3") }

func TestOfficialDefaults(t *testing.T) {
	c := testClient()
	if c.OpenAIURL != "https://api.openai.com/v1/audio/transcriptions" || c.ElevenLabsURL != "https://api.elevenlabs.io/v1/text-to-speech/" {
		t.Fatal("unexpected provider endpoints")
	}
	if c.HTTP.Timeout != 2*time.Minute || c.HTTP.CheckRedirect == nil {
		t.Fatal("missing bounded request or redirect policy")
	}
}

func TestTranscribeSendsOnlyAudioAndModel(t *testing.T) {
	c := testClient()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/audio/transcriptions" {
			t.Error("wrong transcription operation")
		}
		if r.Header.Get("Authorization") != "Bearer fake-transcription-key" {
			t.Error("missing API authorization")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error("invalid multipart form")
			w.WriteHeader(400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if len(r.MultipartForm.Value) != 2 || r.FormValue("model") != "gpt-4o-mini-transcribe" || r.FormValue("response_format") != "json" {
			t.Error("unexpected transcription fields")
		}
		if len(r.MultipartForm.File) != 1 || len(r.MultipartForm.File["file"]) != 1 {
			t.Error("missing audio file")
			w.WriteHeader(400)
			return
		}
		f, h, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		}
		if h.Filename != "message.wav" || string(b) != "synthetic audio fixture" {
			t.Error("audio or private filename handling incorrect")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"text":"  Can you hear me? \n"}`)
	}))
	defer s.Close()
	c.OpenAIURL = s.URL + "/v1/audio/transcriptions"
	got, err := c.Transcribe(context.Background(), inputFile(t))
	if err != nil {
		t.Fatal(err)
	}
	if got != "Can you hear me?" {
		t.Fatalf("wrong transcript: %q", got)
	}
}

func TestSpeechUsesOrdinaryTTSEndpointAndPrivateAtomicOutput(t *testing.T) {
	c := testClient()
	c.Config.VoiceID = "voice/escaped"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/v1/text-to-speech/voice%2Fescaped" || r.URL.Query().Get("output_format") != "mp3_22050_32" {
			t.Error("wrong ordinary TTS operation")
		}
		if r.Header.Get("xi-api-key") != "fake-speech-key" || r.Header.Get("Accept") != "audio/mpeg" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("wrong TTS request headers")
		}
		var got map[string]string
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		if len(got) != 2 || got["text"] != "Testing one two three." || got["model_id"] != "eleven_flash_v2_5" {
			t.Error("wrong TTS payload")
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Write([]byte{1, 2, 3, 4})
	}))
	defer s.Close()
	c.ElevenLabsURL = s.URL + "/v1/text-to-speech/"
	path := outputFile(t)
	if err := os.WriteFile(path, []byte("previous"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := c.Synthesize(context.Background(), "Testing one two three.", path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte{1, 2, 3, 4}) {
		t.Fatal("audio response changed")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatal("speech output is not private")
	}
	assertNoTemps(t, path)
}

func TestHTTPErrorsDoNotEchoResponsesOrCredentials(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			c := testClient()
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				io.WriteString(w, "private-marker-response fake-transcription-key fake-speech-key")
			}))
			defer s.Close()
			c.OpenAIURL = s.URL
			c.ElevenLabsURL = s.URL + "/"
			_, err := c.Transcribe(context.Background(), inputFile(t))
			assertPrivateError(t, err)
			path := outputFile(t)
			err = c.Synthesize(context.Background(), "hello", path)
			assertPrivateError(t, err)
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("failed request created speech file")
			}
			assertNoTemps(t, path)
		})
	}
}

func TestTranscriptionRejectsInvalidResponsesAndOversizedInput(t *testing.T) {
	for name, body := range map[string]string{"bad JSON": "private-marker-not-json", "empty": `{"text":" "}`, "missing": `{"message":"private-marker"}`, "large": `{"text":"` + strings.Repeat("x", 1<<20) + `"}`} {
		t.Run(name, func(t *testing.T) {
			c := testClient()
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
			defer s.Close()
			c.OpenAIURL = s.URL
			_, err := c.Transcribe(context.Background(), inputFile(t))
			assertPrivateError(t, err)
		})
	}
	c := testClient()
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer s.Close()
	c.OpenAIURL = s.URL
	path := inputFile(t)
	if err := os.Truncate(path, maxTranscriptionBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Transcribe(context.Background(), path); err == nil {
		t.Fatal("oversized input accepted")
	}
	if hits.Load() != 0 {
		t.Fatal("oversized input sent to provider")
	}
}

func TestSpeechFailurePreservesDestinationAndCleansTemporaryFiles(t *testing.T) {
	for _, kind := range []string{"empty", "oversized", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			c := testClient()
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "empty":
					return
				case "oversized":
					io.CopyN(w, zeroReader{}, maxSpeechBytes+1)
				case "truncated":
					w.Header().Set("Content-Length", "100")
					io.WriteString(w, "short")
				}
			}))
			defer s.Close()
			c.ElevenLabsURL = s.URL + "/"
			path := outputFile(t)
			if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
				t.Fatal(err)
			}
			assertPrivateError(t, c.Synthesize(context.Background(), "hello", path))
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != "existing" {
				t.Fatal("failed synthesis destroyed existing file")
			}
			assertNoTemps(t, path)
		})
	}
}

func TestRedirectsDoNotForwardProviderKeys(t *testing.T) {
	var destinationHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationHits.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	c := testClient()
	c.OpenAIURL = source.URL
	c.ElevenLabsURL = source.URL + "/"
	_, err := c.Transcribe(context.Background(), inputFile(t))
	assertPrivateError(t, err)
	assertPrivateError(t, c.Synthesize(context.Background(), "hello", outputFile(t)))
	if destinationHits.Load() != 0 {
		t.Fatal("provider redirect was followed")
	}
}

func TestCancellationInterruptsBothProviderRequests(t *testing.T) {
	for _, kind := range []string{"transcribe", "synthesize"} {
		t.Run(kind, func(t *testing.T) {
			started := make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
			}))
			defer s.Close()
			c := testClient()
			c.OpenAIURL = s.URL
			c.ElevenLabsURL = s.URL + "/"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			input, path := inputFile(t), outputFile(t)
			go func() {
				if kind == "transcribe" {
					_, err := c.Transcribe(ctx, input)
					result <- err
				} else {
					result <- c.Synthesize(ctx, "hello", path)
				}
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not start")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation identity lost: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("request did not stop")
			}
			assertNoTemps(t, path)
		})
	}
}

func TestMissingCredentialsNeverMakeRequests(t *testing.T) {
	c := New(config.Config{})
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer s.Close()
	c.OpenAIURL = s.URL
	c.ElevenLabsURL = s.URL + "/"
	if _, err := c.Transcribe(context.Background(), inputFile(t)); err == nil {
		t.Fatal("missing OpenAI key accepted")
	}
	if err := c.Synthesize(context.Background(), "hello", outputFile(t)); err == nil {
		t.Fatal("missing speech credentials accepted")
	}
	if hits.Load() != 0 {
		t.Fatal("request made without credentials")
	}
}

func assertPrivateError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected operation failure")
	}
	for _, marker := range []string{"private-marker", "fake-transcription-key", "fake-speech-key"} {
		if strings.Contains(err.Error(), marker) {
			t.Fatal("error disclosed private response or credentials")
		}
	}
}
func assertNoTemps(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != filepath.Base(path) {
			t.Fatal("temporary file was retained")
		}
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestCancellationDuringResponseBodyPreservesOldAudio(t *testing.T) {
	for _, kind := range []string{"transcribe", "synthesize"} {
		t.Run(kind, func(t *testing.T) {
			read := make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				io.WriteString(w, `{"text":"partial`)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer s.Close()
			c := testClient()
			c.OpenAIURL = s.URL
			c.ElevenLabsURL = s.URL + "/"
			c.HTTP.Transport = notifyReadTransport{read}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			input, path := inputFile(t), outputFile(t)
			if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
				t.Fatal(err)
			}
			go func() {
				if kind == "transcribe" {
					_, err := c.Transcribe(ctx, input)
					result <- err
				} else {
					result <- c.Synthesize(ctx, "hello", path)
				}
			}()
			select {
			case <-read:
			case <-time.After(3 * time.Second):
				t.Fatal("response was not read")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("body cancellation identity lost: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("response did not stop")
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != "existing" {
				t.Fatal("canceled request replaced old audio")
			}
			assertNoTemps(t, path)
		})
	}
}

type notifyReadTransport struct{ read chan struct{} }

func (n notifyReadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err == nil {
		resp.Body = &notifyReadBody{ReadCloser: resp.Body, read: n.read}
	}
	return resp, err
}

type notifyReadBody struct {
	io.ReadCloser
	read chan struct{}
	once sync.Once
}

func (n *notifyReadBody) Read(p []byte) (int, error) {
	count, err := n.ReadCloser.Read(p)
	if count > 0 {
		n.once.Do(func() { close(n.read) })
	}
	return count, err
}
