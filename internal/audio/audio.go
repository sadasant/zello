// Package audio keeps Zello's Opus framing and external media conversion behind
// the service boundary. Ogg encapsulation follows RFC 3533 and RFC 7845.
package audio

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	maxBytes       = 32 << 20
	maxDuration    = 10 * time.Minute
	maxPacketBytes = 64 << 10
)

// SaveIncoming preserves the original packet bytes in a playable Ogg Opus file.
// duration is the duration of one Zello packet, not the complete message. The
// Channel API has no pre-skip or end-trim field, so neither is invented here.
func SaveIncoming(path string, header []byte, duration time.Duration, packets [][]byte) error {
	if len(header) != 4 || (header[2] != 1 && header[2] != 2) || len(packets) == 0 {
		return errors.New("invalid incoming audio header or empty message")
	}
	rate := binary.LittleEndian.Uint16(header[:2])
	switch rate {
	case 8000, 12000, 16000, 24000, 48000:
	default:
		return errors.New("unsupported incoming audio sample rate")
	}
	if duration <= 0 || duration > 60*time.Millisecond || len(packets) > int(maxDuration/duration) {
		return errors.New("incoming audio exceeds duration limit")
	}
	total := 0
	for _, p := range packets {
		frames, frameDuration, err := packetTiming(p)
		if err != nil {
			return err
		}
		// A Channel API uint8 frame-size field cannot represent 2.5 exactly;
		// the official JS implementation stores its integer part (2).
		if frames != int(header[2]) || byte(frameDuration/time.Millisecond) != header[3] || frameDuration*time.Duration(frames) != duration {
			return errors.New("incoming audio timing differs from its header")
		}
		total += len(p)
		if total > maxBytes {
			return errors.New("incoming audio exceeds size limit")
		}
	}
	var serialBytes [4]byte
	if _, err := rand.Read(serialBytes[:]); err != nil {
		return errors.New("cannot create audio stream identifier")
	}
	serial := binary.LittleEndian.Uint32(serialBytes[:])
	return writePrivate(path, func(w io.Writer) error {
		head := make([]byte, 19)
		copy(head, "OpusHead")
		head[8], head[9] = 1, 1 // Version 1, mono, no pre-skip or channel mapping.
		binary.LittleEndian.PutUint32(head[12:16], uint32(rate))
		tags := append([]byte("OpusTags"), 5, 0, 0, 0)
		tags = append(tags, []byte("zello")...)
		tags = append(tags, 0, 0, 0, 0)
		seq := uint32(0)
		if err := writePacket(w, head, 2, 0, serial, &seq); err != nil {
			return err
		}
		if err := writePacket(w, tags, 0, 0, serial, &seq); err != nil {
			return err
		}
		for i, p := range packets {
			flags := byte(0)
			if i == len(packets)-1 {
				flags = 4
			}
			granule := uint64(i+1) * uint64(duration*48000/time.Second)
			if err := writePacket(w, p, flags, granule, serial, &seq); err != nil {
				return err
			}
		}
		return nil
	})
}

// Encode converts local TTS audio to Zello-compatible mono Opus. duration is the
// packet duration, always 20ms. Channel API cannot transmit Ogg pre-skip/end trim;
// preserving every encoder packet adds at most a small encoder delay/padding.
func Encode(ctx context.Context, inputPath string) (header []byte, duration time.Duration, packets [][]byte, err error) {
	var out bytes.Buffer
	if err = convert(ctx, inputPath, &limitedWriter{w: &out, remaining: maxBytes},
		"-ac", "1", "-ar", "16000", "-c:a", "libopus", "-application", "voip", "-b:a", "24k", "-frame_duration", "20", "-vbr", "on", "-f", "opus", "pipe:1"); err != nil {
		return nil, 0, nil, err
	}
	all, err := readOgg(out.Bytes())
	if err != nil {
		return nil, 0, nil, err
	}
	if len(all) < 3 || len(all[0]) != 19 || string(all[0][:8]) != "OpusHead" || all[0][8] != 1 || all[0][9] != 1 || all[0][18] != 0 || binary.LittleEndian.Uint32(all[0][12:16]) != 16000 || !bytes.HasPrefix(all[1], []byte("OpusTags")) {
		return nil, 0, nil, errors.New("encoder produced unsupported audio headers")
	}
	packets = all[2:]
	duration = 20 * time.Millisecond
	if len(packets) > int(maxDuration/duration) {
		return nil, 0, nil, errors.New("outgoing audio exceeds 10 minute limit")
	}
	for _, p := range packets {
		frames, frameDuration, err := packetTiming(p)
		if err != nil {
			return nil, 0, nil, err
		}
		if frames != 1 || frameDuration != duration {
			return nil, 0, nil, errors.New("encoder produced unexpected packet timing")
		}
	}
	return []byte{0x80, 0x3e, 1, 20}, duration, packets, nil
}

// Normalize creates a private, mono 16kHz WAV file for transcription.
func Normalize(ctx context.Context, inputPath, outputPath string) error {
	in, err := filepath.Abs(inputPath)
	if err != nil {
		return errors.New("invalid input audio path")
	}
	out, err := filepath.Abs(outputPath)
	if err != nil || in == out {
		return errors.New("output audio requires a different path")
	}
	return writePrivate(outputPath, func(w io.Writer) error {
		return convert(ctx, inputPath, &limitedWriter{w: w, remaining: int64(maxDuration/time.Second)*16000*2 + 4096},
			"-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-f", "wav", "pipe:1")
	})
}

func convert(ctx context.Context, path string, out io.Writer, args ...string) error {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > maxBytes {
		return errors.New("input audio is missing, empty, or exceeds 32 MiB")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return errors.New("invalid input audio path")
	}
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return errors.New("ffmpeg is required; install with brew install ffmpeg")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// No URL input protocols and no ffmpeg output in errors: audio file contents
	// or metadata could otherwise become diagnostics containing private text.
	base := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-protocol_whitelist", "file,pipe", "-i", path, "-map", "0:a:0", "-vn", "-map_metadata", "-1"}
	cmd := exec.CommandContext(ctx, ffmpeg, append(base, args...)...)
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("audio conversion interrupted: %w", ctx.Err())
		}
		return errors.New("audio conversion failed (invalid audio, output limit, or unavailable encoder)")
	}
	return nil
}

type limitedWriter struct {
	w         io.Writer
	remaining int64
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errors.New("audio size limit exceeded")
	}
	n, err := w.w.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func writePrivate(path string, write func(io.Writer) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".zello-audio-*")
	if err != nil {
		return errors.New("cannot create private audio file")
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return errors.New("cannot persist audio file")
	}
	if err := f.Close(); err != nil {
		return errors.New("cannot close audio file")
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return errors.New("cannot install audio file")
	}
	return nil
}

// packetTiming reads RFC 6716 section 3.1 packet framing without decoding audio.
func packetTiming(p []byte) (frames int, frameDuration time.Duration, err error) {
	if len(p) < 1 || len(p) > maxPacketBytes {
		return 0, 0, errors.New("invalid Opus packet size")
	}
	config := p[0] >> 3
	switch {
	case config < 12:
		frameDuration = []time.Duration{10, 20, 40, 60}[config%4] * time.Millisecond
	case config < 16:
		frameDuration = []time.Duration{10, 20}[config%2] * time.Millisecond
	default:
		frameDuration = []time.Duration{2500, 5000, 10000, 20000}[config%4] * time.Microsecond
	}
	switch p[0] & 3 {
	case 0:
		frames = 1
	case 1, 2:
		frames = 2
	case 3:
		if len(p) < 2 {
			return 0, 0, errors.New("incomplete Opus frame count")
		}
		frames = int(p[1] & 63)
	}
	if frames == 0 || frameDuration*time.Duration(frames) > 120*time.Millisecond {
		return 0, 0, errors.New("invalid Opus packet duration")
	}
	return frames, frameDuration, nil
}

// A packet may need multiple pages. A length divisible by 255 must end with a
// zero-length segment; 255 means continuation, never a packet boundary.
func writePacket(w io.Writer, packet []byte, flags byte, granule uint64, serial uint32, seq *uint32) error {
	laces := make([]byte, len(packet)/255+1)
	for i := range laces {
		laces[i] = 255
	}
	laces[len(laces)-1] = byte(len(packet) % 255)
	continued := false
	for len(laces) > 0 {
		n := min(len(laces), 255)
		size := 0
		for _, l := range laces[:n] {
			size += int(l)
		}
		pageFlags, pageGranule := flags, granule
		if n < len(laces) {
			pageFlags &^= 4
			pageGranule = ^uint64(0)
		}
		if continued {
			pageFlags = (pageFlags &^ 2) | 1
		}
		page := makePage(pageFlags, pageGranule, serial, *seq, laces[:n], packet[:size])
		if _, err := w.Write(page); err != nil {
			return errors.New("cannot write audio page")
		}
		*seq++
		laces, packet, continued = laces[n:], packet[size:], true
	}
	return nil
}

func makePage(flags byte, granule uint64, serial, seq uint32, laces, body []byte) []byte {
	p := make([]byte, 27+len(laces)+len(body))
	copy(p, "OggS")
	p[5], p[26] = flags, byte(len(laces))
	binary.LittleEndian.PutUint64(p[6:14], granule)
	binary.LittleEndian.PutUint32(p[14:18], serial)
	binary.LittleEndian.PutUint32(p[18:22], seq)
	copy(p[27:], laces)
	copy(p[27+len(laces):], body)
	binary.LittleEndian.PutUint32(p[22:26], oggCRC(p))
	return p
}

func oggCRC(p []byte) uint32 {
	var crc uint32
	for _, b := range p {
		crc ^= uint32(b) << 24
		for i := 0; i < 8; i++ {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// readOgg accepts one complete stream, verifying checksums, stream/page identity,
// and lacing across pages. It does not assume that pages contain one packet.
func readOgg(data []byte) ([][]byte, error) {
	if len(data) > maxBytes {
		return nil, errors.New("encoded audio exceeds size limit")
	}
	var packets [][]byte
	var partial []byte
	var serial, seq uint32
	first, ended, continuing := true, false, false
	for len(data) != 0 {
		if ended || len(data) < 27 || string(data[:4]) != "OggS" || data[4] != 0 || data[5]&^byte(7) != 0 {
			return nil, errors.New("invalid Ogg page")
		}
		n := int(data[26])
		if len(data) < 27+n {
			return nil, errors.New("truncated Ogg segment table")
		}
		size := 0
		for _, l := range data[27 : 27+n] {
			size += int(l)
		}
		if len(data) < 27+n+size {
			return nil, errors.New("truncated Ogg page")
		}
		page := append([]byte(nil), data[:27+n+size]...)
		wantCRC := binary.LittleEndian.Uint32(page[22:26])
		clear(page[22:26])
		if oggCRC(page) != wantCRC {
			return nil, errors.New("invalid Ogg checksum")
		}
		pageSerial, pageSeq := binary.LittleEndian.Uint32(page[14:18]), binary.LittleEndian.Uint32(page[18:22])
		if first {
			if page[5]&2 == 0 || pageSeq != 0 {
				return nil, errors.New("missing Ogg stream start")
			}
			serial, seq, first = pageSerial, pageSeq, false
		} else if pageSerial != serial || pageSeq != seq+1 || page[5]&2 != 0 {
			return nil, errors.New("discontinuous Ogg stream")
		}
		seq = pageSeq
		if (page[5]&1 != 0) != continuing {
			return nil, errors.New("broken Ogg packet continuation")
		}
		body := page[27+n:]
		for _, l := range page[27 : 27+n] {
			partial = append(partial, body[:int(l)]...)
			body = body[int(l):]
			if len(partial) > 1<<20 {
				return nil, errors.New("Ogg packet exceeds size limit")
			}
			continuing = l == 255
			if !continuing {
				packets = append(packets, partial)
				partial = nil
			}
		}
		ended = page[5]&4 != 0
		data = data[27+n+size:]
	}
	if first || !ended || continuing {
		return nil, errors.New("incomplete Ogg stream")
	}
	return packets, nil
}
