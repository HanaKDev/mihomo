package splithttp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/metacubex/mihomo/common/utils"
)

type managedPacketWriter struct {
	ctx       context.Context
	url       string
	config    *SplitHTTPConfig
	sessionID string
	shared    *XmuxClient
	seq       int64
	closed    bool
}

func (w *managedPacketWriter) Write(b []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	seqStr := strconv.FormatInt(w.seq, 10)
	w.seq++
	if err := w.shared.dialerClient().PostPacket(w.ctx, w.url, w.sessionID, seqStr, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *managedPacketWriter) Close() error {
	w.closed = true
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
	key := sharedClientKey(config, httpVersion)
	shared := globalClientManager.acquire(ctx, key, config, func() DialerClient {
		return createHTTPClient(config, httpVersion)
	})

	reader, remoteAddr, localAddr, err := shared.dialerClient().OpenStream(ctx, url, sessionID, body, uploadOnly)
	if err != nil {
		shared.release()
		return nil, nil, nil, nil, err
	}
	return reader, remoteAddr, localAddr, shared, nil
}

func dialWithVersion(ctx context.Context, config *SplitHTTPConfig, httpVersion string) (net.Conn, error) {
	mode := parseMode(config)

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

	var sharedUpload *XmuxClient
	var sharedDownload *XmuxClient
	var remoteAddr net.Addr
	var localAddr net.Addr
	var err error

	releaseAll := func() {
		if sharedUpload != nil {
			sharedUpload.release()
		}
		if sharedDownload != nil && sharedDownload != sharedUpload {
			sharedDownload.release()
		}
	}

	if mode == "stream-one" {
		var body io.ReadCloser
		body, remoteAddr, localAddr, sharedUpload, err = openSharedStream(ctx, config, httpVersion, uploadURL, sessionID, reader, false)
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
	downBody, remoteAddr, localAddr, sharedDownload, err = openSharedStream(ctx, downloadConfig, httpVersion, downloadURL, sessionID, nil, false)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}

	if mode == "stream-up" {
		if downloadConfig == config {
			sharedUpload = sharedDownload
			_, _, _, err = sharedUpload.dialerClient().OpenStream(ctx, uploadURL, sessionID, reader, true)
		} else {
			_, _, _, sharedUpload, err = openSharedStream(ctx, config, httpVersion, uploadURL, sessionID, reader, true)
		}
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

	packetWriter := &managedPacketWriter{
		ctx:       ctx,
		url:       uploadURL,
		config:    config,
		sessionID: sessionID,
		shared:    sharedDownload,
	}
	if downloadConfig != config {
		sharedUpload = globalClientManager.acquire(ctx, sharedClientKey(config, httpVersion), config, func() DialerClient {
			return createHTTPClient(config, httpVersion)
		})
		packetWriter.shared = sharedUpload
	}
	return &managedConn{
		writer:     packetWriter,
		reader:     downBody,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
		onClose:    releaseAll,
	}, nil
}

func DialContext(ctx context.Context, config *SplitHTTPConfig) (net.Conn, error) {
	if config.DialTransport == nil {
		return nil, fmt.Errorf("splithttp transport dial is not configured")
	}
	var lastErr error
	for _, httpVersion := range candidateHTTPVersions(config) {
		conn, err := dialWithVersion(ctx, config, httpVersion)
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
