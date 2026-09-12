package service

import (
	"sync"
	"time"
)

const nativeQuiet = 5 * time.Second

type nativeCandidate struct {
	stream   uint32
	id       string
	deadline time.Time
}

// nativeMatcher is connection-local. Receive callbacks are serialized, but
// outgoing activity can arrive from the send goroutine. Methods only decide
// eligibility; callers persist audio/text and schedule fallback outside the lock.
type nativeMatcher struct {
	mu           sync.Mutex
	active       map[uint32]bool
	outgoing     int
	pending      nativeCandidate
	blockedUntil time.Time
	grace        time.Duration
	now          func() time.Time
}

func newNativeMatcher(grace time.Duration) *nativeMatcher {
	return &nativeMatcher{active: map[uint32]bool{}, grace: grace, now: time.Now}
}

// block disqualifies every stream involved in an ambiguous interval. A stream
// that starts during quarantine remains ineligible even if it ends much later.
func (m *nativeMatcher) block(now time.Time) string {
	id := m.pending.id
	m.pending = nativeCandidate{}
	m.blockedUntil = now.Add(nativeQuiet)
	for stream := range m.active {
		m.active[stream] = false
	}
	return id
}

func (m *nativeMatcher) expire(now time.Time) string {
	if m.pending.id != "" && !now.Before(m.pending.deadline) {
		return m.block(m.pending.deadline)
	}
	return ""
}

func (m *nativeMatcher) start(stream uint32) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	id := m.expire(now)
	eligible := len(m.active) == 0 && m.outgoing == 0 && m.pending.id == "" && !now.Before(m.blockedUntil)
	if !eligible {
		if pending := m.block(now); pending != "" {
			id = pending
		}
	}
	m.active[stream] = eligible
	return id
}

func (m *nativeMatcher) finish(stream uint32, id string, valid bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	eligible := valid && m.active[stream] && len(m.active) == 1 && m.outgoing == 0 && !now.Before(m.blockedUntil)
	delete(m.active, stream)
	if !eligible {
		m.block(now)
		return false
	}
	m.pending = nativeCandidate{stream: stream, id: id, deadline: now.Add(m.grace)}
	return true
}

// take deliberately uses a heuristic, not an assertion of identity. A successful
// match also starts a quiet interval to reduce duplicate/late-event reuse.
func (m *nativeMatcher) take(complete bool) nativeCandidate {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	m.expire(now)
	candidate := m.pending
	if !complete || len(m.active) != 0 || m.outgoing != 0 || now.Before(m.blockedUntil) {
		candidate = nativeCandidate{}
	}
	m.block(now)
	return candidate
}

func (m *nativeMatcher) keyed(stream uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending.id != "" && m.pending.stream == stream {
		m.block(m.now())
	}
}

func (m *nativeMatcher) transmit(start bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if start {
		m.outgoing++
	} else {
		m.outgoing--
	}
	return m.block(m.now())
}
