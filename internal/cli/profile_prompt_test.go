package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"testing"
	"time"
)

const profileFixtureJSON = `{"ZELLO_USERNAME":"fixture-user","ZELLO_PASSWORD":"fixture-password","ZELLO_CHANNEL":"fixture-channel","OPENAI_API_KEY":"fixture-openai","ELEVENLABS_API_KEY":"fixture-eleven","ELEVENLABS_VOICE_ID":"fixture-voice"}`

func TestProfileInputReadsJSONWithoutPrompts(t *testing.T) {
	for _, forced := range []bool{false, true} {
		var prompts bytes.Buffer
		cfg, err := readProfileInput(context.Background(), strings.NewReader(profileFixtureJSON), &prompts, forced)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Network != "sadasant" || cfg.Username != "fixture-user" || cfg.Password != "fixture-password" || cfg.Channel != "fixture-channel" || cfg.OpenAIKey != "fixture-openai" || cfg.ElevenLabsKey != "fixture-eleven" || cfg.VoiceID != "fixture-voice" || cfg.TranscribeModel != "gpt-4o-mini-transcribe" || cfg.SpeechModel != "eleven_flash_v2_5" {
			t.Fatal("JSON profile values or defaults did not survive decoding")
		}
		if prompts.Len() != 0 {
			t.Fatal("nonterminal profile input emitted output")
		}
	}
}

func TestProfileInputDoesNotEchoMalformedJSONOrReadErrors(t *testing.T) {
	for _, input := range []io.Reader{
		strings.NewReader(`{"ZELLO_PASSWORD":"private-marker-unclosed`),
		strings.NewReader(`{"unexpected":"private-marker-value"}`),
		strings.NewReader(strings.Repeat(" ", profileInputLimit+1)),
		profileFailingReader{},
	} {
		var prompts bytes.Buffer
		_, err := readProfileInput(context.Background(), input, &prompts, true)
		if err == nil || strings.Contains(err.Error(), "private-marker") {
			t.Fatal("invalid input was accepted or included in its error")
		}
		if prompts.Len() != 0 {
			t.Fatal("failed JSON input emitted output")
		}
	}
}

type profileFailingReader struct{}

func (profileFailingReader) Read([]byte) (int, error) {
	return 0, errors.New("private-marker-reader-error")
}

func TestProfileInputCancellationUnblocksAJSONPipe(t *testing.T) {
	pipe, writer := io.Pipe()
	reader := &profileBlockingReader{PipeReader: pipe, started: make(chan struct{})}
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := readProfileInput(ctx, reader, io.Discard, true)
		done <- err
	}()
	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("profile input never began reading")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("profile input did not preserve cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("profile input remained blocked after cancellation")
	}
}

type profileBlockingReader struct {
	*io.PipeReader
	started chan struct{}
	once    sync.Once
}

func (r *profileBlockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	return r.PipeReader.Read(p)
}

// Python's standard-library pty and termios modules create an isolated real
// terminal. Only synthetic credentials reach this helper subprocess; no user
// terminal, saved credentials, or provider endpoint is touched.
func TestProfilePromptRealTerminalPrivacyAndRestoration(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is needed for the isolated PTY regression")
	}
	for _, mode := range []string{"interactive", "cancel", "json", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", profilePTYScript, os.Args[0], mode)
			cmd.Env = append(os.Environ(), "ZELLO_PROFILE_PTY_HELPER=1", "ZELLO_PROFILE_PTY_MODE="+mode)
			if output, err := cmd.CombinedOutput(); err != nil {
				// The helper deliberately reports fixed failure labels, never its
				// captured terminal stream, profile values, or prompt contents.
				t.Fatalf("isolated terminal regression: %v (%s)", err, strings.TrimSpace(string(output)))
			}
		})
	}
}

func TestProfilePromptPTYHelper(t *testing.T) {
	if os.Getenv("ZELLO_PROFILE_PTY_HELPER") != "1" {
		t.Skip("helper only runs in an isolated terminal subprocess")
	}
	mode := os.Getenv("ZELLO_PROFILE_PTY_MODE")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cfg, err := readProfileInput(ctx, os.Stdin, os.Stderr, mode == "json")
	if mode == "cancel" {
		if !errors.Is(err, context.Canceled) {
			t.Fatal("canceled terminal input did not return cancellation")
		}
		fmt.Println("profile-canceled")
		return
	}
	if mode == "invalid" {
		if err == nil {
			t.Fatal("invalid terminal profile accepted")
		}
		fmt.Println("profile-rejected")
		return
	}
	if err != nil {
		t.Fatal("terminal profile input failed")
	}
	if cfg.Network != "sadasant" || cfg.Username != "fixture-user" || cfg.Password != "fixture-password" || cfg.Channel != "fixture-channel" || cfg.OpenAIKey != "fixture-openai" || cfg.ElevenLabsKey != "fixture-eleven" || cfg.VoiceID != "fixture-voice" || cfg.TranscribeModel != "gpt-4o-mini-transcribe" || cfg.SpeechModel != "eleven_flash_v2_5" {
		t.Fatal("terminal profile did not preserve values and defaults")
	}
	fmt.Println("profile-decoded")
}

const profilePTYScript = `
import errno, json, os, pty, select, signal, subprocess, sys, termios, time
master, slave = pty.openpty()
original = termios.tcgetattr(slave)
# A nondefault local setting makes the assertion check full restoration, not
# merely whether echo happens to have been turned back on.
original[3] |= termios.ECHONL
termios.tcsetattr(slave, termios.TCSANOW, original)
mode = sys.argv[2]
p = subprocess.Popen([sys.argv[1], '-test.run=^TestProfilePromptPTYHelper$'],
                     stdin=slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
prompts = b''
echo = b''
def wait_for(label):
    global prompts, echo
    until = time.monotonic() + 6
    while label not in prompts:
        if time.monotonic() > until:
            raise RuntimeError('prompt timeout')
        ready, _, _ = select.select([p.stderr, master], [], [], .1)
        for fd in ready:
            block = os.read(fd.fileno() if hasattr(fd, 'fileno') else fd, 4096)
            if not block:
                raise RuntimeError('prompt ended early')
            if fd == master: echo += block
            else: prompts += block
try:
    if mode == 'json':
        until = time.monotonic()+6
        while termios.tcgetattr(slave)[3] & termios.ECHO:
            if time.monotonic()>until: raise RuntimeError('echo remained enabled')
            time.sleep(.01)
        values = {'ZELLO_USERNAME':'fixture-user','ZELLO_PASSWORD':'fixture-password',
                  'ZELLO_CHANNEL':'fixture-channel','OPENAI_API_KEY':'fixture-openai',
                  'ELEVENLABS_API_KEY':'fixture-eleven','ELEVENLABS_VOICE_ID':'fixture-voice'}
        os.write(master,json.dumps(values).encode()+b'\n\x04')
    else:
        fields = [('ZELLO_NETWORK', '' if mode != 'invalid' else 'invalid/network'),
                  ('ZELLO_USERNAME','fixture-user'), ('ZELLO_PASSWORD','fixture-password'),
                  ('ZELLO_CHANNEL','fixture-channel'), ('OPENAI_API_KEY','fixture-openai'),
                  ('OPENAI_TRANSCRIBE_MODEL',''), ('ELEVENLABS_API_KEY','fixture-eleven'),
                  ('ELEVENLABS_VOICE_ID','fixture-voice'), ('ELEVENLABS_MODEL_ID','')]
        for label, value in fields:
            wait_for((label + (':' if label in ['ZELLO_USERNAME','ZELLO_PASSWORD','ZELLO_CHANNEL'] else ' ')).encode())
            if mode == 'cancel' and label == 'ZELLO_PASSWORD':
                os.write(master,b'fixture-password')
                os.kill(p.pid,signal.SIGINT)
                break
            os.write(master,value.encode()+b'\n')
    stdout, stderr = p.communicate(timeout=6)
    prompts += stderr
    while select.select([master],[],[],.05)[0]:
        echo += os.read(master,4096)
    if p.returncode != 0: raise RuntimeError('profile helper failed')
    if termios.tcgetattr(slave) != original: raise RuntimeError('terminal settings were not restored')
    expected = {'cancel':b'profile-canceled','invalid':b'profile-rejected'}.get(mode,b'profile-decoded')
    if expected not in stdout: raise RuntimeError('missing result marker')
    for secret in [b'fixture-password',b'fixture-openai',b'fixture-eleven']:
        if secret in echo+prompts+stdout: raise RuntimeError('credential appeared in terminal output')
    if mode == 'json' and prompts: raise RuntimeError('stdin JSON unexpectedly prompted')
except Exception as error:
    # Deliberately print only controlled labels; captured streams could contain
    # synthetic secrets when a regression occurs.
    print(str(error) if isinstance(error,RuntimeError) else 'PTY helper failure',file=sys.stderr)
    p.kill()
    p.wait()
    sys.exit(1)
finally:
    os.close(master)
    os.close(slave)
`
