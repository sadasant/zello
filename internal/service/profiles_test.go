package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sadasant/zello/internal/channel"
	"github.com/sadasant/zello/internal/config"
	"github.com/sadasant/zello/internal/ipc"
	"github.com/sadasant/zello/internal/store"
)

func TestNamedProfilesRunIndependentServices(t *testing.T) {
	seed := makeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	type runningProfile struct {
		paths  config.Paths
		cfg    config.Config
		db     *store.Store
		voice  *transportFake
		result chan error
	}
	var running []*runningProfile
	t.Cleanup(func() {
		cancel()
		for _, p := range running {
			select {
			case err := <-p.result:
				if err != nil {
					t.Errorf("profile shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("profile shutdown timed out")
			}
			p.db.Close()
		}
	})
	for _, name := range []string{"Peter", "Maria"} {
		cfg := config.Config{Username: "fixture-" + name, Password: "fixture-password", Channel: "fixture-" + name}
		if err := config.SaveProfile(seed.paths, name, cfg); err != nil {
			t.Fatal(err)
		}
		paths, err := config.SelectProfile(seed.paths, name)
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := config.Load(paths)
		if err != nil {
			t.Fatal(err)
		}
		if err = config.Prepare(paths); err != nil {
			t.Fatal(err)
		}
		db, err := store.Open(paths.DB)
		if err != nil {
			t.Fatal(err)
		}
		voice := &transportFake{ready: make(chan struct{}), events: make(chan transportEvent, 8), sent: make(chan channel.Incoming, 8)}
		p := &runningProfile{paths: paths, cfg: loaded, db: db, voice: voice, result: make(chan error, 1)}
		running = append(running, p)
		svc := &Service{Paths: paths, Config: loaded, Store: db, Speech: &speechFake{wav: seed.speech.wav}, NewTransport: func(cb channel.Callbacks) Transport { voice.cb = cb; return voice }}
		go func() { p.result <- svc.Run(ctx) }()
		select {
		case <-voice.ready:
		case err := <-p.result:
			p.result <- err
			t.Fatalf("profile startup: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("profile startup timed out")
		}
		var status ipc.Status
		if err = ipc.Request(ctx, paths.Socket, "status", &status); err != nil || !status.Running || status.State != "connected" {
			t.Fatalf("profile health: %+v, %v", status, err)
		}
	}
	peter, maria := running[0], running[1]
	if peter.paths.Socket == maria.paths.Socket || peter.paths.DB == maria.paths.DB || filepath.Dir(peter.paths.Socket) != peter.paths.Data {
		t.Fatal("profiles alias local state")
	}
	deliver(t, peter.voice, transportEvent{text: &channel.TextMessage{Sender: "person", Channel: peter.cfg.Channel, Text: "only Peter"}})
	if n, err := peter.db.Count(ctx); err != nil || n != 1 {
		t.Fatalf("Peter unread: %d, %v", n, err)
	}
	if n, err := maria.db.Count(ctx); err != nil || n != 0 {
		t.Fatalf("Maria received Peter's message: %d, %v", n, err)
	}
	for _, p := range running {
		outgoing, err := p.db.Enqueue(ctx, "hello "+p.paths.Profile, p.cfg.Channel)
		if err != nil {
			t.Fatal(err)
		}
		var response struct{}
		if err = ipc.Request(ctx, p.paths.Socket, "wake", &response); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool { m, err := p.db.Show(ctx, outgoing.ID); return err == nil && m.Status == "sent" })
		if len(p.voice.sent) != 1 {
			t.Fatal("outgoing message did not reach the selected transport")
		}
	}
	// The same canonical profile still permits only one service, even when another
	// profile is also running on the machine.
	duplicate := &Service{Paths: peter.paths, Config: peter.cfg, Store: peter.db, Speech: &speechFake{wav: seed.speech.wav}}
	if err := duplicate.Run(ctx); err == nil {
		t.Fatal("duplicate profile service acquired the lock")
	}
	for _, p := range running {
		var status ipc.Status
		if err := ipc.Request(ctx, p.paths.Socket, "status", &status); err != nil || !status.Running {
			t.Fatal("duplicate startup disturbed a running profile")
		}
	}
}
