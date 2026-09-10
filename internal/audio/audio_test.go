package audio

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFFmpegVoiceRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	input, preserved, wav := filepath.Join(dir, "speech.wav"), filepath.Join(dir, "incoming.opus"), filepath.Join(dir, "normalized.wav")
	if err := os.WriteFile(input, toneWAV(), 0600); err != nil {
		t.Fatal(err)
	}
	header, duration, packets, err := Encode(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(header, []byte{0x80, 0x3e, 1, 20}) || duration != 20*time.Millisecond {
		t.Fatalf("wrong wire format: %x, %v", header, duration)
	}
	if len(packets) < 50 || len(packets) > 52 {
		t.Fatalf("unexpected encoded duration: %d packets", len(packets))
	}
	if err := SaveIncoming(preserved, header, duration, packets); err != nil {
		t.Fatal(err)
	}
	ogg, err := os.ReadFile(preserved)
	if err != nil {
		t.Fatal(err)
	}
	extracted, err := readOgg(ogg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(extracted[2:], packets) {
		t.Fatal("incoming packets were changed")
	}
	if binary.LittleEndian.Uint16(extracted[0][10:12]) != 0 {
		t.Fatal("invented source pre-skip")
	}
	if err := Normalize(context.Background(), preserved, wav); err != nil {
		t.Fatal(err)
	}
	decoded, err := os.ReadFile(wav)
	if err != nil {
		t.Fatal(err)
	}
	pcm := wavData(t, decoded)
	if len(pcm) < 32000 || len(pcm) > 34000 {
		t.Fatalf("decoded length = %d, want about one second", len(pcm))
	}
	var energy float64
	crossings := 0
	for i := 2; i < len(pcm); i += 2 {
		previous := int16(binary.LittleEndian.Uint16(pcm[i-2:]))
		sample := int16(binary.LittleEndian.Uint16(pcm[i:]))
		energy += float64(sample) * float64(sample)
		if previous <= 0 && sample > 0 {
			crossings++
		}
	}
	if energy/float64(len(pcm)/2) < 1e6 || crossings < 425 || crossings > 460 {
		t.Fatalf("bad decoded tone: energy=%g, cycles=%d", energy, crossings)
	}
	for _, path := range []string{preserved, wav} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("audio mode = %o", info.Mode().Perm())
		}
	}
}

func TestOggLacingAcrossPagesAndMultiplePackets(t *testing.T) {
	// A 65025-byte packet exactly fills 255 laces. Its terminating zero lace
	// must occur on the following continuation page.
	large := bytes.Repeat([]byte{99}, 65025)
	var out bytes.Buffer
	seq := uint32(0)
	if err := writePacket(&out, large, 2, 960, 9, &seq); err != nil {
		t.Fatal(err)
	}
	out.Write(makePage(4, 2880, 9, seq, []byte{3, 0, 4}, []byte("onetwo!")))
	got, err := readOgg(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{large, []byte("one"), nil, []byte("two!")}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("packet boundaries were lost")
	}
	if seq != 2 {
		t.Fatalf("full packet consumed %d pages, want 2", seq)
	}
}

func TestOggRejectsCorruptionAndIncompleteStreams(t *testing.T) {
	var out bytes.Buffer
	seq := uint32(0)
	if err := writePacket(&out, []byte("packet"), 6, 960, 123, &seq); err != nil {
		t.Fatal(err)
	}
	valid := out.Bytes()
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-1] ^= 1
	cases := map[string][]byte{
		"empty": nil, "truncated": valid[:len(valid)-1], "checksum": corrupt,
		"no end":              makePage(2, 960, 123, 0, []byte{1}, []byte{7}),
		"orphan continuation": makePage(7, 960, 123, 0, []byte{1}, []byte{7}),
		"unterminated packet": makePage(6, 960, 123, 0, []byte{255}, make([]byte, 255)),
		"chained":             append(append([]byte(nil), valid...), valid...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readOgg(data); err == nil {
				t.Fatal("accepted corrupt stream")
			}
		})
	}
	first := makePage(2, 960, 123, 0, []byte{1}, []byte{7})
	for name, last := range map[string][]byte{
		"wrong serial": makePage(4, 1920, 124, 1, []byte{1}, []byte{7}),
		"sequence gap": makePage(4, 1920, 123, 2, []byte{1}, []byte{7}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := readOgg(append(append([]byte(nil), first...), last...)); err == nil {
				t.Fatal("accepted discontinuity")
			}
		})
	}
}

func TestOfficialCodecHeaderAndTiming(t *testing.T) {
	// The official Channel API specification's gD4BPA== is 16kHz, one 60ms
	// frame. Its illustrative packet_duration=20 conflicts with this header;
	// derive and validate timing from the real header and Opus TOC instead.
	header, err := base64.StdEncoding.DecodeString("gD4BPA==")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "source.opus")
	if err := SaveIncoming(path, header, 60*time.Millisecond, [][]byte{{3 << 3, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := SaveIncoming(path, header, 20*time.Millisecond, [][]byte{{3 << 3, 0}}); err == nil {
		t.Fatal("accepted conflicting packet duration")
	}
	cases := []struct {
		p        []byte
		frames   int
		duration time.Duration
	}{
		{[]byte{1 << 3}, 1, 20 * time.Millisecond},
		{[]byte{16 << 3}, 1, 2500 * time.Microsecond},
		{[]byte{1<<3 | 1}, 2, 20 * time.Millisecond},
		{[]byte{1<<3 | 3, 2}, 2, 20 * time.Millisecond},
	}
	for _, c := range cases {
		f, d, err := packetTiming(c.p)
		if err != nil || f != c.frames || d != c.duration {
			t.Fatalf("timing %x: %d %v %v", c.p, f, d, err)
		}
	}
	for _, p := range [][]byte{nil, {3}, {3, 0}, {3, 63}} {
		if _, _, err := packetTiming(p); err == nil {
			t.Fatalf("accepted invalid packet %x", p)
		}
	}
}

func TestPrivateOutputPreservesOldFileOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.wav")
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	err := Normalize(context.Background(), "/nonexistent/input.wav", path)
	if err == nil {
		t.Fatal("accepted nonexistent input")
	}
	got, _ := os.ReadFile(path)
	if string(got) != "existing" {
		t.Fatal("failed conversion destroyed existing file")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary file leaked: %v %v", entries, err)
	}
}

func TestConversionErrorDoesNotExposeMediaOrPaths(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	path := filepath.Join(t.TempDir(), "private-marker-input")
	if err := os.WriteFile(path, []byte("private-marker-content"), 0600); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := Encode(context.Background(), path)
	if err == nil || strings.Contains(err.Error(), "private-marker") {
		t.Fatalf("unsafe conversion error: %v", err)
	}
	if err := os.WriteFile(path, toneWAV(), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = Encode(ctx, path)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
}

func toneWAV() []byte {
	const rate = 16000
	b := make([]byte, 44+rate*2)
	copy(b, "RIFF")
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-8))
	copy(b[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(b[16:], 16)
	binary.LittleEndian.PutUint16(b[20:], 1)
	binary.LittleEndian.PutUint16(b[22:], 1)
	binary.LittleEndian.PutUint32(b[24:], rate)
	binary.LittleEndian.PutUint32(b[28:], rate*2)
	binary.LittleEndian.PutUint16(b[32:], 2)
	binary.LittleEndian.PutUint16(b[34:], 16)
	copy(b[36:], "data")
	binary.LittleEndian.PutUint32(b[40:], rate*2)
	for i := 0; i < rate; i++ {
		binary.LittleEndian.PutUint16(b[44+i*2:], uint16(int16(math.Sin(float64(i)*2*math.Pi*440/rate)*10000)))
	}
	return b
}

func wavData(t *testing.T, b []byte) []byte {
	t.Helper()
	if len(b) < 12 || string(b[:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		t.Fatal("not WAV")
	}
	for i := 12; i+8 <= len(b); {
		size := binary.LittleEndian.Uint32(b[i+4:])
		if string(b[i:i+4]) == "data" {
			return b[i+8:]
		} // Pipe output may have unknown length.
		i += 8 + int(size) + (int(size) % 2)
	}
	t.Fatal("WAV missing samples")
	return nil
}
