package splithttp

import (
	"context"
	"time"
)

var dialerClientFactory = createHTTPClient

type xmuxLease struct {
	ctx         context.Context
	key         string
	config      *SplitHTTPConfig
	httpVersion string
	client      *XmuxClient
	create      func() DialerClient
}

func newXmuxLease(ctx context.Context, config *SplitHTTPConfig, httpVersion string) *xmuxLease {
	return &xmuxLease{
		ctx:         ctx,
		key:         sharedClientKey(config, httpVersion),
		config:      config,
		httpVersion: httpVersion,
		create: func() DialerClient {
			return dialerClientFactory(config, httpVersion)
		},
	}
}

func (l *xmuxLease) acquire() {
	if l.client != nil {
		return
	}
	l.client = globalClientManager.acquire(l.ctx, l.key, l.config, l.create)
}

func (l *xmuxLease) dialerClient() DialerClient {
	if l.client == nil {
		return nil
	}
	return l.client.dialerClient()
}

func (l *xmuxLease) release() {
	if l.client == nil {
		return
	}
	l.client.release()
	l.client = nil
}

func (l *xmuxLease) consumeStreamRequest() {
	if l.client != nil {
		l.client.LeftRequests.Add(-1)
	}
}

func (l *xmuxLease) rotateForPacket(now time.Time) {
	l.acquire()
	if l.client == nil {
		return
	}
	expired := !l.client.UnreusableAt.IsZero() && now.After(l.client.UnreusableAt)
	if l.client.XmuxConn.IsClosed() || expired || l.client.LeftRequests.Add(-1) <= 0 {
		next := globalClientManager.acquire(l.ctx, l.key, l.config, l.create)
		l.client.release()
		l.client = next
		l.client.LeftRequests.Add(-1)
	}
}
