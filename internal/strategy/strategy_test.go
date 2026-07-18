package strategy

import (
	"context"
	"orderbook/internal/clock"
	"orderbook/internal/pipeline"
	"orderbook/internal/ring"
	"testing"
)

type capture struct {
	recv int64
	v    uint64
}

func (c *capture) OnEvent(e pipeline.Event, r int64) { c.recv = r; c.v = e.Version }
func TestReceiveBoundary(t *testing.T) {
	q, err := ring.NewSPSC[pipeline.Event](2)
	if err != nil {
		t.Fatal(err)
	}
	q.TryPublish(pipeline.Event{Version: 7})
	_ = q.Close()
	c := &capture{}
	if err := Run(context.Background(), q, c, clock.NewSim(300)); err != nil {
		t.Fatal(err)
	}
	if c.recv != 300 || c.v != 7 {
		t.Fatalf("%+v", c)
	}
}
