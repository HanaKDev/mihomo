package splithttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
)

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
			return createHTTPClient(config, httpVersion)
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

type managedPacketWriter struct {
	ctx       context.Context
	url       string
	config    *SplitHTTPConfig
	sessionID string
	lease     *xmuxLease

	maxUploadSize    int
	maxBufferedBytes int
	minPostInterval  RangeConfig

	mu       sync.Mutex
	cond     *sync.Cond
	buffer   bytes.Buffer
	seq      int64
	closed   bool
	writeErr error
	done     chan struct{}
	lastPost time.Time
}

type DialRuntime struct {
	HasReality bool
}

func newManagedPacketWriter(ctx context.Context, url string, config *SplitHTTPConfig, sessionID string, lease *xmuxLease) *managedPacketWriter {
	maxUploadSize := config.GetNormalizedScMaxEachPostBytes().rand()
	if maxUploadSize <= 0 {
		maxUploadSize = 1
	}
	maxBufferedBytes := config.GetNormalizedScMaxBufferedPosts() * maxUploadSize
	w := &managedPacketWriter{
		ctx:              ctx,
		url:              url,
		config:           config,
		sessionID:        sessionID,
		lease:            lease,
		maxUploadSize:    maxUploadSize,
		maxBufferedBytes: maxBufferedBytes,
		minPostInterval:  config.GetNormalizedScMinPostsInterval(),
		done:             make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	go w.run()
	return w
}

func (w *managedPacketWriter) nextChunk() ([]byte, string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		if w.writeErr != nil {
			return nil, "", w.writeErr
		}
		if w.buffer.Len() > 0 {
			size := w.maxUploadSize
			if size <= 0 || size > w.buffer.Len() {
				size = w.buffer.Len()
			}
			chunk := make([]byte, size)
			_, _ = w.buffer.Read(chunk)
			seqStr := strconv.FormatInt(w.seq, 10)
			w.seq++
			w.cond.Broadcast()
			return chunk, seqStr, nil
		}
		if w.closed {
			return nil, "", io.EOF
		}
		w.cond.Wait()
	}
}

func (w *managedPacketWriter) fail(err error) {
	w.mu.Lock()
	if w.writeErr == nil {
		w.writeErr = err
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *managedPacketWriter) run() {
	defer close(w.done)

	for {
		chunk, seqStr, err := w.nextChunk()
		if err != nil {
			return
		}
		if waitMs := w.minPostInterval.rand(); waitMs > 0 {
			sleepFor := time.Duration(waitMs)*time.Millisecond - time.Since(w.lastPost)
			if sleepFor > 0 {
				timer := time.NewTimer(sleepFor)
				select {
				case <-w.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
		w.lastPost = time.Now()
		w.lease.rotateForPacket(w.lastPost)
		if err := w.lease.dialerClient().PostPacket(w.ctx, w.url, w.sessionID, seqStr, chunk); err != nil {
			w.fail(err)
			return
		}
	}
}

func (w *managedPacketWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		if w.writeErr != nil {
			return 0, w.writeErr
		}
		if w.closed {
			return 0, io.ErrClosedPipe
		}
		if w.maxBufferedBytes <= 0 || w.buffer.Len()+len(b) <= w.maxBufferedBytes {
			break
		}
		w.cond.Wait()
	}

	_, _ = w.buffer.Write(b)
	w.cond.Signal()
	return len(b), nil
}

func (w *managedPacketWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.cond.Broadcast()
	w.mu.Unlock()
	<-w.done
	return nil
}

func candidateHTTPVersions(config *SplitHTTPConfig) []string {
	seen := map[string]bool{}
	out := make([]string, 0, 3)
	appendVersion := func(v string) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}

	if config.TryQUIC && config.HasALPN("h3") {
		appendVersion("3")
	}
	if len(config.ALPN) == 0 || config.HasALPN("h2") || config.HasALPN("http/1.1") {
		appendVersion("2")
	}
	if config.HasALPN("http/1.1") || config.HasALPN("http/1.0") {
		appendVersion("1.1")
	}
	if len(out) == 0 {
		appendVersion("2")
	}
	return out
}

func sharedClientKey(config *SplitHTTPConfig, httpVersion string) string {
	return config.ClientKey + "|" + httpVersion
}

func openSharedStream(ctx context.Context, config *SplitHTTPConfig, httpVersion, url, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, *XmuxClient, error) {
	lease := newXmuxLease(ctx, config, httpVersion)
	lease.acquire()
	lease.consumeStreamRequest()

	reader, remoteAddr, localAddr, err := lease.dialerClient().OpenStream(ctx, url, sessionID, body, uploadOnly)
	if err != nil {
		lease.release()
		return nil, nil, nil, nil, err
	}
	return reader, remoteAddr, localAddr, lease.client, nil
}

func openSharedStreamWithLease(ctx context.Context, lease *xmuxLease, url, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	lease.acquire()
	lease.consumeStreamRequest()

	reader, remoteAddr, localAddr, err := lease.dialerClient().OpenStream(ctx, url, sessionID, body, uploadOnly)
	if err != nil {
		lease.release()
		return nil, nil, nil, err
	}
	return reader, remoteAddr, localAddr, nil
}

func newSharedStreamLease(ctx context.Context, config *SplitHTTPConfig, httpVersion string) *xmuxLease {
	lease := newXmuxLease(ctx, config, httpVersion)
	lease.acquire()
	return lease
}

func resolveDialMode(config *SplitHTTPConfig, runtime DialRuntime) string {
	if config.Mode != "" && config.Mode != "auto" {
		return config.Mode
	}
	if runtime.HasReality {
		if config.DownloadConfig != nil {
			return "stream-up"
		}
		return "stream-one"
	}
	return "packet-up"
}

func dialWithVersion(ctx context.Context, config *SplitHTTPConfig, httpVersion string, runtime DialRuntime) (net.Conn, error) {
	mode := resolveDialMode(config, runtime)

	sessionID := ""
	if mode != "stream-one" {
		sessionID = utils.NewUUIDV4().String()
	}

	uploadURL := fmt.Sprintf("https://%s%s", config.Host, config.GetNormalizedPath())
	downloadConfig := config
	if config.DownloadConfig != nil {
		downloadConfig = config.DownloadConfig
	}
	downloadURL := fmt.Sprintf("https://%s%s", downloadConfig.Host, downloadConfig.GetNormalizedPath())
	reader, writer := io.Pipe()

	uploadLease := newXmuxLease(ctx, config, httpVersion)
	downloadLease := newXmuxLease(ctx, downloadConfig, httpVersion)

	var remoteAddr net.Addr
	var localAddr net.Addr
	var err error

	releaseAll := func() {
		uploadLease.release()
		downloadLease.release()
	}

	if mode == "stream-one" {
		var body io.ReadCloser
		body, remoteAddr, localAddr, err = openSharedStreamWithLease(ctx, uploadLease, uploadURL, sessionID, reader, false)
		if err != nil {
			_ = reader.Close()
			_ = writer.Close()
			return nil, err
		}
		return &managedConn{
			writer:     writer,
			reader:     body,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
			onClose:    releaseAll,
		}, nil
	}

	var downBody io.ReadCloser
	downBody, remoteAddr, localAddr, err = openSharedStreamWithLease(ctx, downloadLease, downloadURL, sessionID, nil, false)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}

	if mode == "stream-up" {
		_, _, _, err = openSharedStreamWithLease(ctx, uploadLease, uploadURL, sessionID, reader, true)
		if err != nil {
			_ = downBody.Close()
			_ = reader.Close()
			_ = writer.Close()
			releaseAll()
			return nil, err
		}
		return &managedConn{
			writer:     writer,
			reader:     downBody,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
			onClose:    releaseAll,
		}, nil
	}

	packetWriter := newManagedPacketWriter(ctx, uploadURL, config, sessionID, uploadLease)
	return &managedConn{
		writer:     packetWriter,
		reader:     downBody,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
		onClose:    releaseAll,
	}, nil
}

func DialContextWithOptions(ctx context.Context, config *SplitHTTPConfig, runtime DialRuntime) (net.Conn, error) {
	if config.DialTransport == nil {
		return nil, fmt.Errorf("splithttp transport dial is not configured")
	}
	var lastErr error
	for _, httpVersion := range candidateHTTPVersions(config) {
		conn, err := dialWithVersion(ctx, config, httpVersion, runtime)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("splithttp failed to select a transport")
	}
	return nil, lastErr
}

func DialContext(ctx context.Context, config *SplitHTTPConfig) (net.Conn, error) {
	return DialContextWithOptions(ctx, config, DialRuntime{})
}
