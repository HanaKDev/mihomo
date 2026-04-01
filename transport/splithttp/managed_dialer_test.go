package splithttp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type fakePacketDialerClient struct {
	mu      sync.Mutex
	closed  bool
	posts   []fakePacketPost
	openErr error
	postErr error
}

type fakePacketPost struct {
	sessionID string
	seq       string
	payload   string
}

func (f *fakePacketDialerClient) IsClosed() bool { return f.closed }

func (f *fakePacketDialerClient) OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	return nil, nil, nil, f.openErr
}

func (f *fakePacketDialerClient) PostPacket(_ context.Context, _ string, sessionID string, seqStr string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postErr != nil {
		return f.postErr
	}
	f.posts = append(f.posts, fakePacketPost{
		sessionID: sessionID,
		seq:       seqStr,
		payload:   string(payload),
	})
	return nil
}

func TestResolveDialMode(t *testing.T) {
	tests := []struct {
		name    string
		config  *SplitHTTPConfig
		runtime DialRuntime
		want    string
	}{
		{name: "explicit", config: &SplitHTTPConfig{Mode: "stream-up"}, runtime: DialRuntime{}, want: "stream-up"},
		{name: "auto-default", config: &SplitHTTPConfig{Mode: "auto"}, runtime: DialRuntime{}, want: "packet-up"},
		{name: "auto-reality", config: &SplitHTTPConfig{Mode: "auto"}, runtime: DialRuntime{HasReality: true}, want: "stream-one"},
		{name: "auto-reality-download", config: &SplitHTTPConfig{Mode: "auto", DownloadConfig: &SplitHTTPConfig{}}, runtime: DialRuntime{HasReality: true}, want: "stream-up"},
		{name: "empty-default", config: &SplitHTTPConfig{}, runtime: DialRuntime{}, want: "packet-up"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveDialMode(tt.config, tt.runtime); got != tt.want {
				t.Fatalf("resolveDialMode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestManagedPacketWriterSplitsPayload(t *testing.T) {
	client := &fakePacketDialerClient{}
	lease := &xmuxLease{
		ctx:    context.Background(),
		key:    "test",
		config: &SplitHTTPConfig{},
		client: &XmuxClient{XmuxConn: client},
	}
	lease.client.LeftRequests.Store(1 << 30)

	writer := newManagedPacketWriter(context.Background(), "https://example.com/test", &SplitHTTPConfig{
		ScMaxEachPostBytes: &RangeConfig{From: 4, To: 4},
		ScMaxBufferedPosts: 4,
		ScMinPostsInterval: &RangeConfig{},
	}, "session-1", lease)

	if _, err := writer.Write([]byte("abcdefghij")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.posts) != 3 {
		t.Fatalf("expected 3 posts, got %d", len(client.posts))
	}
	want := []fakePacketPost{
		{sessionID: "session-1", seq: "0", payload: "abcd"},
		{sessionID: "session-1", seq: "1", payload: "efgh"},
		{sessionID: "session-1", seq: "2", payload: "ij"},
	}
	for i := range want {
		if client.posts[i] != want[i] {
			t.Fatalf("post %d = %+v, want %+v", i, client.posts[i], want[i])
		}
	}
}

func TestXmuxLeaseRotatesAfterRequestBudget(t *testing.T) {
	previousManager := globalClientManager
	globalClientManager = &clientManager{clients: map[string]*XmuxManager{}}
	defer func() { globalClientManager = previousManager }()

	created := make([]*fakePacketDialerClient, 0, 2)
	cfg := &SplitHTTPConfig{
		ClientKey: "rotation",
		Xmux: &XmuxConfig{
			HMaxRequestTimes: &RangeConfig{From: 2, To: 2},
		},
	}
	lease := newXmuxLease(context.Background(), cfg, "2")
	lease.create = func() DialerClient {
		client := &fakePacketDialerClient{}
		created = append(created, client)
		return client
	}

	lease.acquire()
	first := lease.client
	lease.rotateForPacket(time.Now())
	if lease.client != first {
		t.Fatalf("expected first request to keep current client")
	}
	lease.rotateForPacket(time.Now())
	if lease.client == first {
		t.Fatalf("expected request budget exhaustion to rotate client")
	}
	if len(created) != 2 {
		t.Fatalf("expected 2 created clients, got %d", len(created))
	}
}
