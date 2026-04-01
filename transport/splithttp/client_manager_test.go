package splithttp

import (
	"context"
	"testing"
)

type fakeXmuxConn struct{}

func (f *fakeXmuxConn) IsClosed() bool { return false }

func TestXmuxManagerMaxConnections(t *testing.T) {
	manager := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 8; i++ {
		clients[manager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(clients) != 4 {
		t.Fatalf("expected 4 distinct xmux clients, got %d", len(clients))
	}
}

func TestXmuxManagerCMaxReuseTimes(t *testing.T) {
	manager := NewXmuxManager(XmuxConfig{
		CMaxReuseTimes: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 64; i++ {
		clients[manager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(clients) != 32 {
		t.Fatalf("expected 32 distinct xmux clients, got %d", len(clients))
	}
}

func TestXmuxManagerMaxConcurrency(t *testing.T) {
	manager := NewXmuxManager(XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 64; i++ {
		client := manager.GetXmuxClient(context.Background())
		client.OpenUsage.Add(1)
		clients[client] = struct{}{}
	}

	if len(clients) != 32 {
		t.Fatalf("expected 32 distinct xmux clients, got %d", len(clients))
	}
}

func TestXmuxManagerDefaultReuse(t *testing.T) {
	manager := NewXmuxManager(XmuxConfig{}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 64; i++ {
		client := manager.GetXmuxClient(context.Background())
		client.OpenUsage.Add(1)
		clients[client] = struct{}{}
	}

	if len(clients) != 1 {
		t.Fatalf("expected 1 distinct xmux client, got %d", len(clients))
	}
}
