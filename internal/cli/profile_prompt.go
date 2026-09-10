package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sadasant/zello/internal/config"
)

const profileInputLimit = 64 << 10

// readProfileInput never writes input values to diagnostics. Interactive secret
// input is hidden; redirected input uses the same strict JSON profile decoder.
func readProfileInput(ctx context.Context, in io.Reader, prompts io.Writer, forceStdin bool) (cfg config.Config, err error) {
	terminal, err := profileTerminalFor(ctx, in)
	if err != nil {
		return cfg, err
	}
	if terminal != nil {
		defer func() {
			if restoreErr := terminal.restore(); restoreErr != nil {
				cfg, err = config.Config{}, restoreErr
			}
		}()
	}
	if forceStdin || terminal == nil {
		if terminal != nil {
			if err := terminal.hide(ctx); err != nil {
				return cfg, err
			}
		}
		body, err := readProfilePart(ctx, in, terminal, func() ([]byte, error) {
			return io.ReadAll(io.LimitReader(in, profileInputLimit+1))
		})
		if err != nil {
			return cfg, err
		}
		return config.DecodeProfile(bytes.NewReader(body))
	}

	fields := []struct {
		key, fallback string
		secret        bool
	}{
		{"ZELLO_NETWORK", "sadasant", false},
		{"ZELLO_USERNAME", "", false},
		{"ZELLO_PASSWORD", "", true},
		{"ZELLO_CHANNEL", "", false},
		{"OPENAI_API_KEY", "", true},
		{"OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe", false},
		{"ELEVENLABS_API_KEY", "", true},
		{"ELEVENLABS_VOICE_ID", "", false},
		{"ELEVENLABS_MODEL_ID", "eleven_flash_v2_5", false},
	}
	values := make(map[string]string, len(fields))
	reader := bufio.NewReader(io.LimitReader(in, profileInputLimit+1))
	total := 0
	for _, field := range fields {
		if err := ctx.Err(); err != nil {
			return cfg, err
		}
		if field.secret {
			if err := terminal.hide(ctx); err != nil {
				return cfg, err
			}
		}
		prompt := field.key
		if field.fallback != "" {
			prompt += " [" + field.fallback + "]"
		} else if strings.HasPrefix(field.key, "OPENAI_") || strings.HasPrefix(field.key, "ELEVENLABS_") {
			prompt += " (optional)"
		}
		if _, err := fmt.Fprint(prompts, prompt+": "); err != nil {
			return cfg, errors.New("cannot write profile prompt")
		}
		line, err := readProfilePart(ctx, in, terminal, func() ([]byte, error) { return reader.ReadBytes('\n') })
		if err != nil {
			return cfg, err
		}
		total += len(line)
		if total > profileInputLimit {
			return cfg, errors.New("profile input exceeds 64 KiB")
		}
		if field.secret {
			if err := terminal.restore(); err != nil {
				return cfg, err
			}
			if _, err := fmt.Fprintln(prompts); err != nil {
				return cfg, errors.New("cannot write profile prompt")
			}
		}
		value := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
		if value == "" {
			value = field.fallback
		}
		values[field.key] = value
	}
	body, err := json.Marshal(values)
	if err != nil {
		return cfg, errors.New("cannot encode profile input")
	}
	return config.DecodeProfile(bytes.NewReader(body))
}

type profileTerminal struct {
	file     *os.File
	original string
	changed  bool
}

// /bin/test -t and stty -g inspect the descriptor without changing terminal
// state. A failed terminal inspection never falls through to an echoed JSON read.
func profileTerminalFor(ctx context.Context, in io.Reader) (*profileTerminal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, ok := in.(*os.File)
	if !ok {
		return nil, nil
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.New("cannot inspect profile input")
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return nil, nil
	}
	check := exec.CommandContext(ctx, "/bin/test", "-t", "0")
	check.Stdin = file
	if err := check.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return nil, nil
		}
		return nil, errors.New("cannot inspect profile terminal")
	}
	inspect := exec.CommandContext(ctx, "/bin/stty", "-g")
	inspect.Stdin = file
	state, err := inspect.Output()
	if err != nil || len(state) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("cannot read profile terminal settings")
	}
	return &profileTerminal{file: file, original: strings.TrimSpace(string(state))}, nil
}

func (t *profileTerminal) hide(ctx context.Context) error {
	// An interrupted stty may have applied its setting before exiting, so even
	// an unsuccessful command requires restoration of the captured state.
	t.changed = true
	command := exec.CommandContext(ctx, "/bin/stty", "-echo")
	command.Stdin = t.file
	if err := command.Run(); err != nil {
		return errors.New("cannot hide profile input")
	}
	return nil
}

func (t *profileTerminal) restore() error {
	if !t.changed {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/stty", t.original)
	command.Stdin = t.file
	if err := command.Run(); err != nil {
		return errors.New("cannot restore terminal settings; run stty sane")
	}
	t.changed = false
	return nil
}

// Only the reader goroutine touches input. The caller restores terminal state
// before closing a canceled read, while that descriptor is still usable by stty.
func readProfilePart(ctx context.Context, in io.Reader, terminal *profileTerminal, read func() ([]byte, error)) ([]byte, error) {
	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() { body, err := read(); done <- result{body, err} }()
	select {
	case r := <-done:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if r.err != nil {
			return nil, errors.New("cannot read profile input")
		}
		return r.body, nil
	case <-ctx.Done():
		if terminal != nil {
			if err := terminal.restore(); err != nil {
				return nil, err
			}
		}
		if closer, ok := in.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, ctx.Err()
	}
}
