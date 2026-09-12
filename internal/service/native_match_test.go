package service

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/sadasant/zello/internal/channel"
)

func TestSingleCandidateMakesNativeTextReadableWithoutOpenAI(t *testing.T) {
	f := makeFixture(t)
	sink := &logSink{}
	f.logger = log.New(sink, "", 0)
	s, tr, _ := startFixture(t, f, 10*time.Second, nil)
	deliver(t, tr, transportEvent{start: f.voice.StreamID})
	deliver(t, tr, transportEvent{audio: &f.voice})
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{Text: "inferred words"}})
	m := unread(t, f, "inferred words")
	if f.speech.transcriptions.Load() != 0 || !s.native.Load() {
		t.Fatal("native match did not bypass OpenAI")
	}
	if !strings.Contains(sink.String(), "source=native_inferred") {
		t.Fatal("inferred provenance missing")
	}
	if err := f.db.Consume(context.Background(), m.ID); err != nil {
		t.Fatal(err)
	}
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{Text: "duplicate words"}})
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{StreamID: f.voice.StreamID, Text: "late keyed words"}})
	got, err := f.db.Show(context.Background(), m.ID)
	if err != nil || got.Text != "inferred words" || got.Status != "consumed" {
		t.Fatal("late event rewrote consumed message")
	}
}

func TestNewStreamExpeditesOldAudioAndQuarantinesBoth(t *testing.T) {
	f := makeFixture(t)
	s, tr, _ := startFixture(t, f, time.Minute, nil)
	deliver(t, tr, transportEvent{start: f.voice.StreamID})
	deliver(t, tr, transportEvent{audio: &f.voice})
	second := f.voice
	second.StreamID++
	deliver(t, tr, transportEvent{start: second.StreamID})
	// B has only started. A must already fall back, without waiting for B's
	// audio completion or A's one-minute native grace period in this fixture.
	first := unread(t, f, "fallback transcription")
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{Text: "A's late native words"}})
	deliver(t, tr, transportEvent{audio: &second})
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{Text: "B's unkeyed words"}})
	eventually(t, func() bool { n, _ := f.db.Count(context.Background()); return n == 2 })
	msgs, err := f.db.Inbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		if msg.Text != "fallback transcription" {
			t.Fatal("ambiguous words entered inbox")
		}
	}
	if first.ID == msgs[1].ID || s.native.Load() || f.speech.transcriptions.Load() != 2 {
		t.Fatal("overlap did not use two independent fallbacks")
	}
}

func TestOutgoingActivityDisqualifiesPendingNativeCandidate(t *testing.T) {
	f := makeFixture(t)
	_, tr, _ := startFixture(t, f, time.Minute, nil)
	deliver(t, tr, transportEvent{start: f.voice.StreamID})
	deliver(t, tr, transportEvent{audio: &f.voice})
	on := true
	deliver(t, tr, transportEvent{transmit: &on})
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{Text: "outgoing echo"}})
	unread(t, f, "fallback transcription")
	on = false
	deliver(t, tr, transportEvent{transmit: &on})
}

func TestKeyedTranscriptStillWorksDuringQuarantine(t *testing.T) {
	f := makeFixture(t)
	_, tr, _ := startFixture(t, f, time.Second, nil)
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{Text: "orphan"}})
	deliver(t, tr, transportEvent{start: f.voice.StreamID})
	// A keyed early event can be associated even while unidentified events
	// are quarantined; it must complete before fallback is expedited.
	deliver(t, tr, transportEvent{transcript: &channel.Transcript{StreamID: f.voice.StreamID, Text: "identified words"}})
	deliver(t, tr, transportEvent{audio: &f.voice})
	unread(t, f, "identified words")
	if f.speech.transcriptions.Load() != 0 {
		t.Fatal("unnecessary OpenAI fallback for keyed native event")
	}
}

func TestNativeQuietIntervalAndLateEvents(t *testing.T) {
	now := time.Unix(100, 0)
	m := newNativeMatcher(3 * time.Second)
	m.now = func() time.Time { return now }
	m.start(1)
	if !m.finish(1, "A", true) {
		t.Fatal("first recording ineligible")
	}
	if got := m.start(2); got != "A" {
		t.Fatal("A not released to fallback")
	}
	if m.finish(2, "B", true) {
		t.Fatal("overlapping B became eligible")
	}
	now = now.Add(nativeQuiet - time.Second)
	if m.take(true).id != "" {
		t.Fatal("late A transcript matched B")
	}
	// The late event extends quarantine, even though the original deadline
	// has elapsed. Starting during quarantine disqualifies the entire stream.
	now = now.Add(2 * time.Second)
	m.start(3)
	now = now.Add(10 * time.Second)
	if m.finish(3, "C", true) {
		t.Fatal("long stream escaped quarantine")
	}
	now = now.Add(nativeQuiet)
	m.start(4)
	if !m.finish(4, "D", true) || m.take(true).id != "D" {
		t.Fatal("matching did not resume after quiet")
	}
	if m.take(true).id != "" {
		t.Fatal("duplicate matched twice")
	}
}

func TestNativeTimeoutRejectionAndReconnectBoundaries(t *testing.T) {
	for _, kind := range []string{"timeout", "partial", "rejected", "outgoing"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Unix(100, 0)
			m := newNativeMatcher(3 * time.Second)
			m.now = func() time.Time { return now }
			m.start(1)
			m.finish(1, "A", kind != "rejected")
			switch kind {
			case "timeout":
				now = now.Add(3 * time.Second)
			case "partial":
				if m.take(false).id != "" {
					t.Fatal("partial accepted")
				}
			case "outgoing":
				m.transmit(true)
				m.transmit(false)
			}
			if m.take(true).id != "" {
				t.Fatal("ineligible message accepted native words")
			}
			now = now.Add(nativeQuiet)
			m.start(2)
			if !m.finish(2, "B", true) || m.take(true).id != "B" {
				t.Fatal("quiet recovery failed")
			}
		})
	}
	// A new connection owns a fresh stream namespace, with no old candidate.
	if newNativeMatcher(time.Second).take(true).id != "" {
		t.Fatal("orphan transcript accepted after reconnect")
	}
}
