// Package provider makes only transcription and ordinary text-to-speech requests.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sadasant/zello/internal/config"
)

const (
	maxTranscriptionBytes = 25 * 1024 * 1024
	maxSpeechBytes        = 32 * 1024 * 1024
)

type Client struct {
	Config                   config.Config
	HTTP                     *http.Client
	OpenAIURL, ElevenLabsURL string
}

func New(c config.Config) *Client {
	return &Client{c, &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, "https://api.openai.com/v1/audio/transcriptions", "https://api.elevenlabs.io/v1/text-to-speech/"}
}
func (c *Client) Transcribe(ctx context.Context, path string) (string, error) {
	if c.Config.OpenAIKey == "" {
		return "", errors.New("OPENAI_API_KEY missing; native transcript not available")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot open transcription audio")
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() == 0 || stat.Size() > maxTranscriptionBytes {
		return "", errors.New("transcription audio is empty, invalid, or exceeds 25 MiB")
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", "message.wav")
	if err != nil {
		return "", err
	}
	if size, copyErr := io.Copy(part, io.LimitReader(f, maxTranscriptionBytes+1)); copyErr != nil {
		return "", errors.New("cannot read transcription audio")
	} else if size > maxTranscriptionBytes {
		return "", errors.New("transcription audio exceeds 25 MiB")
	}
	_ = form.WriteField("model", c.Config.TranscribeModel)
	_ = form.WriteField("response_format", "json")
	if err = form.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.OpenAIURL, &body)
	if err != nil {
		return "", errors.New("invalid transcription endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+c.Config.OpenAIKey)
	req.Header.Set("Content-Type", form.FormDataContentType())
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", requestError(ctx, "transcription")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("transcription HTTP %d", resp.StatusCode)
	}
	var result struct {
		Text string `json:"text"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&result); err != nil {
		if ctx.Err() != nil {
			return "", requestError(ctx, "transcription")
		}
		return "", errors.New("invalid transcription response")
	}
	result.Text = strings.TrimSpace(result.Text)
	if result.Text == "" {
		return "", errors.New("empty transcription")
	}
	return result.Text, nil
}
func (c *Client) Synthesize(ctx context.Context, text, path string) error {
	if c.Config.ElevenLabsKey == "" || c.Config.VoiceID == "" {
		return errors.New("ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID are required for outgoing speech")
	}
	body, err := json.Marshal(map[string]string{"text": text, "model_id": c.Config.SpeechModel})
	if err != nil {
		return err
	}
	endpoint := c.ElevenLabsURL + url.PathEscape(c.Config.VoiceID) + "?output_format=mp3_22050_32"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid speech endpoint")
	}
	req.Header.Set("xi-api-key", c.Config.ElevenLabsKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "audio/mpeg")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return requestError(ctx, "speech")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("speech HTTP %d", resp.StatusCode)
	}
	f, err := os.CreateTemp(filepath.Dir(path), "speech-*.tmp")
	if err != nil {
		return errors.New("cannot create speech audio")
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	size, copyErr := io.Copy(f, io.LimitReader(resp.Body, maxSpeechBytes+1))
	syncErr := f.Sync()
	closeErr := f.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		if ctx.Err() != nil {
			return requestError(ctx, "speech")
		}
		return errors.New("cannot save speech audio")
	}
	if size == 0 || size > maxSpeechBytes {
		return errors.New("speech audio empty or exceeds 32 MiB")
	}
	if err = os.Rename(tmp, path); err != nil {
		return errors.New("cannot finalize speech audio")
	}
	return nil
}

func requestError(ctx context.Context, operation string) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s request interrupted: %w", operation, ctx.Err())
	}
	return fmt.Errorf("%s request failed", operation)
}
