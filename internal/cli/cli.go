// Package cli exposes text, JSON, and message IDs to ordinary local processes.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sadasant/zello/internal/config"
	"github.com/sadasant/zello/internal/ipc"
	"github.com/sadasant/zello/internal/provider"
	"github.com/sadasant/zello/internal/service"
	"github.com/sadasant/zello/internal/store"
)

var ErrDisconnected = errors.New("disconnected")

const Usage = `Usage: zello <command> [--json]

  service               Maintain the voice connection and process queues
  status                Report connected or disconnected (exit 1 if disconnected)
  count                 Count unread incoming messages ready to read
  inbox                 List unread messages
  peek                  Show oldest unread message without consuming it
  consume <id>          Atomically consume an unread message
  next                  Atomically consume and show oldest unread message
  wait                  Wait, then atomically consume and show a message
  send <text>           Queue text for speech; reads stdin if text is omitted
  show <id>             Show durable message state

Configuration: ~/.config/zello/.env
peek, next, wait, and show produce JSON. Other commands support --json.
`

func Run(ctx context.Context, args []string, in io.Reader, out, diagnostics io.Writer, p config.Paths) error {
	if err := config.Prepare(p); err != nil {
		return err
	}
	cfg, err := config.Load(p)
	if err != nil {
		return err
	}
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		_, err := io.WriteString(out, Usage)
		return err
	}
	command := args[0]
	jsonOutput := false
	var positional []string
	flags := true
	for _, arg := range args[1:] {
		if flags && arg == "--" {
			flags = false
			continue
		}
		if flags && arg == "--json" {
			jsonOutput = true
			continue
		}
		positional = append(positional, arg)
	}
	want := 0
	switch command {
	case "consume", "show":
		want = 1
	case "send":
		want = -1
	case "service", "status", "count", "inbox", "peek", "next", "wait":
	default:
		return fmt.Errorf("unknown command %q; use zello help", command)
	}
	if want >= 0 && len(positional) != want {
		return fmt.Errorf("%s expects %d arguments; use zello help", command, want)
	}
	db, err := store.Open(p.DB)
	if err != nil {
		return err
	}
	defer db.Close()
	encode := func(v any) error { return json.NewEncoder(out).Encode(v) }
	printMessage := func(m store.Message) error { m.AudioPath = ""; return encode(m) }
	switch command {
	case "service":
		f, err := os.OpenFile(p.Log, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		s := service.Service{Config: cfg, Paths: p, Store: db, Speech: provider.New(cfg), Logger: log.New(io.MultiWriter(diagnostics, f), "", log.LstdFlags|log.LUTC)}
		return s.Run(ctx)
	case "status":
		state := ipc.Status{State: "disconnected"}
		if err := ipc.Request(ctx, p.Socket, "status", &state); err != nil {
			state = ipc.Status{State: "disconnected"}
		}
		if jsonOutput {
			err = encode(state)
		} else {
			_, err = fmt.Fprintln(out, state.State)
		}
		if err != nil {
			return err
		}
		if state.State != "connected" {
			return ErrDisconnected
		}
		return nil
	case "count":
		n, err := db.Count(ctx)
		if err != nil {
			return err
		}
		if jsonOutput {
			return encode(map[string]int{"count": n})
		}
		_, err = fmt.Fprintln(out, n)
		return err
	case "inbox":
		messages, err := db.Inbox(ctx)
		if err != nil {
			return err
		}
		for i := range messages {
			messages[i].AudioPath = ""
		}
		if jsonOutput {
			return encode(messages)
		}
		for _, m := range messages {
			if _, err = fmt.Fprintf(out, "%s  %s  %s\n%s\n\n", m.ID, m.CreatedAt.Format(time.RFC3339), m.Sender, m.Text); err != nil {
				return err
			}
		}
		return nil
	case "peek", "next", "show":
		var m store.Message
		switch command {
		case "peek":
			m, err = db.Peek(ctx)
		case "next":
			m, err = db.Next(ctx)
		case "show":
			m, err = db.Show(ctx, positional[0])
		}
		if err != nil {
			return err
		}
		return printMessage(m)
	case "consume":
		id := positional[0]
		if err = db.Consume(ctx, id); err != nil {
			return err
		}
		if jsonOutput {
			return encode(map[string]string{"id": id, "status": "consumed"})
		}
		_, err = fmt.Fprintln(out, id)
		return err
	case "wait":
		for {
			m, err := db.Next(ctx)
			if err == nil {
				return printMessage(m)
			}
			if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			var wake struct{}
			if err = ipc.Request(ctx, p.Socket, "wait", &wake); err != nil {
				timer := time.NewTimer(500 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	case "send":
		if cfg.Channel == "" {
			return errors.New("configure ZELLO_CHANNEL in ~/.config/zello/.env")
		}
		text := strings.Join(positional, " ")
		if len(positional) == 0 {
			body, err := readInput(ctx, in)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("cannot read message from stdin")
			}
			if len(body) > 160000 {
				return errors.New("stdin message exceeds 160000 bytes")
			}
			text = string(body)
		}
		text = strings.TrimSpace(text)
		if text == "" {
			return errors.New("message text must not be empty")
		}
		if !utf8.ValidString(text) || len(text) > 160000 || utf8.RuneCountInString(text) > 40000 {
			return errors.New("message must be valid UTF-8 and at most 40000 characters")
		}
		m, err := db.Enqueue(ctx, text, cfg.Channel)
		if err != nil {
			return err
		}
		// Enqueue is durable even if the service is down or a wake notification fails.
		wakeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		var wake struct{}
		_ = ipc.Request(wakeCtx, p.Socket, "wake", &wake)
		if jsonOutput {
			return encode(map[string]string{"id": m.ID, "status": "queued"})
		}
		_, err = fmt.Fprintln(out, m.ID)
		return err
	}
	return nil
}

// A blocked pipe or terminal read must not swallow the process interrupt.
func readInput(ctx context.Context, in io.Reader) ([]byte, error) {
	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() { body, err := io.ReadAll(io.LimitReader(in, 160001)); done <- result{body, err} }()
	select {
	case r := <-done:
		return r.body, r.err
	case <-ctx.Done():
		if closer, ok := in.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, ctx.Err()
	}
}
