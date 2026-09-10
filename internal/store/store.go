// Package store provides the durable, local queue shared by the service and CLI.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"modernc.org/sqlite"
)

var ErrNotFound = errors.New("message not found or no longer available")

type Message struct {
	ID                    string     `json:"id"`
	Direction             string     `json:"direction"`
	Sender                string     `json:"from"`
	Channel               string     `json:"channel"`
	Text                  string     `json:"text"`
	AudioPath             string     `json:"audio_path,omitempty"`
	Status                string     `json:"status"`
	ReplyTo               string     `json:"reply_to,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	ConsumedAt            *time.Time `json:"consumed_at,omitempty"`
	Error                 string     `json:"error,omitempty"`
	TranscriptionStatus   string     `json:"-"`
	TranscriptionAttempts int        `json:"-"`
	NextTranscriptionAt   time.Time  `json:"-"`
}

type Store struct{ db *sql.DB }

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:], nil
}

func Open(path string) (*Store, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	u := &url.URL{Scheme: "file", Path: path}
	q := u.Query()
	// Driver-level pragmas run on every connection, including newly opened ones.
	for _, pragma := range []string{"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)"} {
		q.Add("_pragma", pragma)
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := initialize(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func initialize(ctx context.Context, db *sql.DB) error {
	const schema = `CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		direction TEXT NOT NULL CHECK(direction IN ('incoming','outgoing')),
		sender TEXT NOT NULL DEFAULT '',
		channel TEXT NOT NULL,
		text TEXT NOT NULL DEFAULT '',
		audio_path TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		reply_to TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		consumed_at INTEGER,
		error TEXT NOT NULL DEFAULT '',
		transcription_status TEXT NOT NULL DEFAULT '',
		transcription_attempts INTEGER NOT NULL DEFAULT 0,
		next_transcription_at INTEGER NOT NULL DEFAULT 0,
		CHECK ((direction='incoming' AND status IN ('unread','consumed')) OR
		       (direction='outgoing' AND status IN ('queued','synthesizing','sending','sent','failed')))
	);
	CREATE INDEX IF NOT EXISTS messages_queue ON messages(direction,status,created_at,id);
	CREATE INDEX IF NOT EXISTS messages_transcription ON messages(transcription_status,next_transcription_at);`
	var lastBusy error
	for delay := 10 * time.Millisecond; ; delay = min(2*delay, 200*time.Millisecond) {
		_, err := db.ExecContext(ctx, schema)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Join(err, lastBusy, ctx.Err())
		}
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != 5 { // SQLITE_BUSY, including extended codes.
			return err
		}
		// Initial WAL selection can return BUSY without invoking busy_timeout.
		// Retry only these idempotent pragmas/schema operations, never queue writes.
		lastBusy = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(lastBusy, ctx.Err())
		case <-timer.C:
		}
	}
}

func (s *Store) Close() error { return s.db.Close() }

const columns = `id,direction,sender,channel,text,audio_path,status,reply_to,created_at,
	consumed_at,error,transcription_status,transcription_attempts,next_transcription_at`

type scanner interface{ Scan(...any) error }

func scan(row scanner) (Message, error) {
	var m Message
	var created, next int64
	var consumed sql.NullInt64
	err := row.Scan(&m.ID, &m.Direction, &m.Sender, &m.Channel, &m.Text, &m.AudioPath,
		&m.Status, &m.ReplyTo, &created, &consumed, &m.Error, &m.TranscriptionStatus,
		&m.TranscriptionAttempts, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return m, ErrNotFound
	}
	if err != nil {
		return m, err
	}
	m.CreatedAt = time.Unix(0, created).UTC()
	if consumed.Valid {
		t := time.Unix(0, consumed.Int64).UTC()
		m.ConsumedAt = &t
	}
	if next != 0 {
		m.NextTranscriptionAt = time.Unix(0, next).UTC()
	}
	return m, nil
}

func changed(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return ErrNotFound
	}
	return err
}

func (s *Store) Enqueue(ctx context.Context, text, channel string) (Message, error) {
	if strings.TrimSpace(text) == "" {
		return Message{}, errors.New("message text must not be empty")
	}
	id, err := NewID()
	if err != nil {
		return Message{}, err
	}
	return scan(s.db.QueryRowContext(ctx, `INSERT INTO messages
		(id,direction,channel,text,status,created_at) VALUES (?,'outgoing',?,?,'queued',?)
		RETURNING `+columns, id, channel, text, time.Now().UnixNano()))
}

// SaveIncoming stores the original audio's locator before transcription starts.
// Pending transcripts remain durable but are invisible to inbox consumers.
func (s *Store) SaveIncoming(ctx context.Context, m Message) error {
	if m.ID == "" {
		return errors.New("incoming message ID must not be empty")
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	var next int64
	if !m.NextTranscriptionAt.IsZero() {
		next = m.NextTranscriptionAt.UnixNano()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO messages
		(id,direction,sender,channel,audio_path,status,reply_to,created_at,
		 transcription_status,next_transcription_at)
		VALUES (?,'incoming',?,?,?,'unread',?,?,'pending',?)`,
		m.ID, m.Sender, m.Channel, m.AudioPath, m.ReplyTo, m.CreatedAt.UnixNano(), next)
	return err
}

// SaveTextIncoming commits a typed message and its readable state together.
// Unlike audio, there is no original file from which to recover a missing body.
func (s *Store) SaveTextIncoming(ctx context.Context, m Message) error {
	if m.ID == "" {
		return errors.New("incoming message ID must not be empty")
	}
	if strings.TrimSpace(m.Text) == "" {
		return errors.New("message text must not be empty")
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO messages
		(id,direction,sender,channel,text,status,reply_to,created_at,transcription_status)
		VALUES (?,'incoming',?,?,?,'unread',?,?,'done')`,
		m.ID, m.Sender, m.Channel, m.Text, m.ReplyTo, m.CreatedAt.UnixNano())
	return err
}

// CompleteIncoming accepts the first successful transcription only. In particular,
// a late native transcript cannot replace text already handed to a consumer.
func (s *Store) CompleteIncoming(ctx context.Context, id, text string) error {
	return changed(s.db.ExecContext(ctx, `UPDATE messages
		SET text=?,transcription_status='done',error='',next_transcription_at=0
		WHERE id=? AND direction='incoming' AND status='unread' AND transcription_status='pending'`, text, id))
}

func (s *Store) FailIncoming(ctx context.Context, id, detail string) error {
	// Retry indefinitely with a bounded delay; neither audio nor unread state is lost.
	return changed(s.db.ExecContext(ctx, `UPDATE messages
		SET error=?,transcription_attempts=transcription_attempts+1,
		next_transcription_at=? + min(300, (1 << min(transcription_attempts+1, 9))) * 1000000000
		WHERE id=? AND direction='incoming' AND status='unread' AND transcription_status='pending'`,
		detail, time.Now().UnixNano(), id))
}

// RejectIncoming retains unusable audio for inspection without scheduling retries.
// Like a late transcription failure, it cannot invalidate an already ready message.
func (s *Store) RejectIncoming(ctx context.Context, id, reason string) error {
	if reason == "" {
		reason = "incoming audio is incomplete or unusable"
	}
	return changed(s.db.ExecContext(ctx, `UPDATE messages
		SET error=?,transcription_status='failed',next_transcription_at=0
		WHERE id=? AND direction='incoming' AND status='unread' AND transcription_status='pending'`, reason, id))
}

func (s *Store) list(ctx context.Context, query string, args ...any) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := []Message{}
	for rows.Next() {
		m, err := scan(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

const unread = `direction='incoming' AND status='unread' AND transcription_status='done'`

func (s *Store) PendingIncoming(ctx context.Context) ([]Message, error) {
	return s.list(ctx, `SELECT `+columns+` FROM messages WHERE direction='incoming'
		AND status='unread' AND transcription_status='pending' AND next_transcription_at<=?
		ORDER BY created_at,id LIMIT 16`, time.Now().UnixNano())
}

func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM messages WHERE `+unread).Scan(&n)
	return n, err
}

func (s *Store) Inbox(ctx context.Context) ([]Message, error) {
	return s.list(ctx, `SELECT `+columns+` FROM messages WHERE `+unread+` ORDER BY created_at,id`)
}

func (s *Store) Peek(ctx context.Context) (Message, error) {
	return scan(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM messages WHERE `+unread+` ORDER BY created_at,id LIMIT 1`))
}

func (s *Store) Next(ctx context.Context) (Message, error) {
	return scan(s.db.QueryRowContext(ctx, `UPDATE messages SET status='consumed',consumed_at=?
		WHERE id=(SELECT id FROM messages WHERE `+unread+` ORDER BY created_at,id LIMIT 1)
		RETURNING `+columns, time.Now().UnixNano()))
}

func (s *Store) Consume(ctx context.Context, id string) error {
	return changed(s.db.ExecContext(ctx, `UPDATE messages SET status='consumed',consumed_at=? WHERE id=? AND `+unread,
		time.Now().UnixNano(), id))
}

func (s *Store) Show(ctx context.Context, id string) (Message, error) {
	return scan(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM messages WHERE id=?`, id))
}

func (s *Store) ClaimOutgoing(ctx context.Context) (Message, error) {
	return scan(s.db.QueryRowContext(ctx, `UPDATE messages SET status='synthesizing',error=''
		WHERE id=(SELECT id FROM messages WHERE direction='outgoing' AND status='queued' ORDER BY created_at,id LIMIT 1)
		RETURNING `+columns))
}

func (s *Store) SetOutgoing(ctx context.Context, id, status, audioPath, detail string) error {
	switch status {
	case "queued", "synthesizing", "sending", "sent", "failed":
	default:
		return fmt.Errorf("invalid outgoing status %q", status)
	}
	return changed(s.db.ExecContext(ctx, `UPDATE messages SET status=?,audio_path=?,error=?
		WHERE id=? AND direction='outgoing'`, status, audioPath, detail, id))
}

// Recover must run only after the caller obtains the exclusive service lock.
// Speech interrupted in flight cannot safely be replayed without a delivery receipt.
func (s *Store) Recover(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE messages SET
		status=CASE status WHEN 'synthesizing' THEN 'queued' ELSE 'failed' END,
		error=CASE status WHEN 'sending' THEN 'delivery status unknown after service interruption; not retried to avoid duplicate speech' ELSE '' END
		WHERE direction='outgoing' AND status IN ('synthesizing','sending')`)
	return err
}
