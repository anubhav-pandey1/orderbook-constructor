package strategy

import (
	"context"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/clock"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/ring"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
	"testing"
)

type capture struct {
	recv int64
	v    uint64
}

func (c *capture) OnEvent(e replay.Event, r int64) { c.recv = r; c.v = e.Version }
func TestReceiveBoundary(t *testing.T) {
	q, err := ring.NewSPSC[replay.Event](2)
	if err != nil {
		t.Fatal(err)
	}
	q.TryPublish(replay.Event{Version: 7})
	_ = q.Close()
	c := &capture{}
	if err := Run(context.Background(), q, c, clock.NewSim(300)); err != nil {
		t.Fatal(err)
	}
	if c.recv != 300 || c.v != 7 {
		t.Fatalf("capture recv/version=%d/%d, want 300/7", c.recv, c.v)
	}
}
