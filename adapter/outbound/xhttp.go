package outbound

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/transport/splithttp"
)

type SplitHTTPOptions struct {
	Host                string                     `proxy:"host,omitempty"`
	Path                string                     `proxy:"path,omitempty"`
	Headers             map[string]string          `proxy:"headers,omitempty"`
	MaxUploadSize       int                        `proxy:"max-upload-size,omitempty"`
	MaxConcurrentPosts  int                        `proxy:"max-concurrent-posts,omitempty"`
	Mode                string                     `proxy:"mode,omitempty"`
	NoGRPCHeader        bool                       `proxy:"no-grpc-header,omitempty"`
	XPaddingBytes       string                     `proxy:"x-padding-bytes,omitempty"`
	XPaddingBytesFrom   int                        `proxy:"x-padding-bytes-from,omitempty"`
	XPaddingBytesTo     int                        `proxy:"x-padding-bytes-to,omitempty"`
	XPaddingObfsMode    bool                       `proxy:"x-padding-obfs-mode,omitempty"`
	XPaddingKey         string                     `proxy:"x-padding-key,omitempty"`
	XPaddingHeader      string                     `proxy:"x-padding-header,omitempty"`
	XPaddingPlacement   string                     `proxy:"x-padding-placement,omitempty"`
	XPaddingMethod      string                     `proxy:"x-padding-method,omitempty"`
	UplinkHTTPMethod    string                     `proxy:"uplink-http-method,omitempty"`
	SessionPlacement    string                     `proxy:"session-placement,omitempty"`
	SessionKey          string                     `proxy:"session-key,omitempty"`
	SeqPlacement        string                     `proxy:"seq-placement,omitempty"`
	SeqKey              string                     `proxy:"seq-key,omitempty"`
	UplinkDataPlacement string                     `proxy:"uplink-data-placement,omitempty"`
	UplinkDataKey       string                     `proxy:"uplink-data-key,omitempty"`
	RequestLog          *bool                      `proxy:"request-log,omitempty"`
	TryQUIC             *bool                      `proxy:"try-quic,omitempty"`
	DownloadSettings    *SplitHTTPDownloadSettings `proxy:"download-settings,omitempty"`
}

type SplitHTTPDownloadSettings struct {
	Path              *string            `proxy:"path,omitempty"`
	Host              *string            `proxy:"host,omitempty"`
	Headers           *map[string]string `proxy:"headers,omitempty"`
	NoGRPCHeader      *bool              `proxy:"no-grpc-header,omitempty"`
	XPaddingBytes     *string            `proxy:"x-padding-bytes,omitempty"`
	Server            *string            `proxy:"server,omitempty"`
	Port              *int               `proxy:"port,omitempty"`
	TLS               *bool              `proxy:"tls,omitempty"`
	ALPN              *[]string          `proxy:"alpn,omitempty"`
	ECHOpts           *ECHOptions        `proxy:"ech-opts,omitempty"`
	RealityOpts       *RealityOptions    `proxy:"reality-opts,omitempty"`
	SkipCertVerify    *bool              `proxy:"skip-cert-verify,omitempty"`
	Fingerprint       *string            `proxy:"fingerprint,omitempty"`
	Certificate       *string            `proxy:"certificate,omitempty"`
	PrivateKey        *string            `proxy:"private-key,omitempty"`
	ServerName        *string            `proxy:"servername,omitempty"`
	ClientFingerprint *string            `proxy:"client-fingerprint,omitempty"`
}

type XHTTPOptions = SplitHTTPOptions
type XHTTPDownloadSettings = SplitHTTPDownloadSettings

func parseCompatXPaddingRange(value string) *splithttp.RangeConfig {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	parts := strings.Split(value, "-")
	switch len(parts) {
	case 1:
		n, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil
		}
		return &splithttp.RangeConfig{From: n, To: n}
	case 2:
		from, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil
		}
		to, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil
		}
		return &splithttp.RangeConfig{From: from, To: to}
	default:
		return nil
	}
}

func normalizeSplitHTTPDialAddr(ctx context.Context, addr string) string {
	_ = ctx
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if net.ParseIP(host) != nil {
		return addr
	}
	return net.JoinHostPort(host, port)
}
func decideXHTTPALPN(alpn []string) []string {
	log.Debugln("decideXHTTPALPN received: %v", alpn)
	if len(alpn) == 0 {
		return []string{"h2"}
	}
	var res []string
	for _, a := range alpn {
		p := strings.ToLower(a)
		if p == "h3" || p == "h2" || p == "http/1.1" {
			res = append(res, p)
		}
	}
	if len(res) == 0 {
		return []string{"h2"}
	}
	return res
}

func buildSplitHTTPClientKey(dialAddr, tlsServerName, host string, alpn []string, tlsEnabled bool) string {
	return fmt.Sprintf("%s|%s|%s|%s|%t", dialAddr, tlsServerName, host, strings.Join(alpn, ","), tlsEnabled)
}

func buildSplitHTTPConfig(ctx context.Context, addr string, tlsServerName string, alpn []string, xhttpOpts SplitHTTPOptions, splitHTTPOpts SplitHTTPOptions, tlsEnabled bool) *splithttp.SplitHTTPConfig {
	host, _, _ := net.SplitHostPort(addr)
	requestLog := log.Level() == log.DEBUG
	if os.Getenv("MIHOMO_XHTTP_DEBUG") == "1" {
		requestLog = true
	}
	if xhttpOpts.RequestLog != nil {
		requestLog = *xhttpOpts.RequestLog
	}
	if splitHTTPOpts.RequestLog != nil {
		requestLog = *splitHTTPOpts.RequestLog
	}
	tryQuic := true
	if xhttpOpts.TryQUIC != nil {
		tryQuic = *xhttpOpts.TryQUIC
	} else if splitHTTPOpts.TryQUIC != nil {
		tryQuic = *splitHTTPOpts.TryQUIC
	}
	config := &splithttp.SplitHTTPConfig{
		Host:                xhttpOpts.Host,
		Path:                xhttpOpts.Path,
		ALPN:                decideXHTTPALPN(alpn),
		DialAddr:            normalizeSplitHTTPDialAddr(ctx, addr),
		TLSServerName:       tlsServerName,
		Headers:             http.Header{},
		MaxUploadSize:       xhttpOpts.MaxUploadSize,
		MaxConcurrentPosts:  xhttpOpts.MaxConcurrentPosts,
		Mode:                xhttpOpts.Mode,
		TLS:                 tlsEnabled,
		XPaddingObfsMode:    xhttpOpts.XPaddingObfsMode,
		XPaddingKey:         xhttpOpts.XPaddingKey,
		XPaddingHeader:      xhttpOpts.XPaddingHeader,
		XPaddingPlacement:   xhttpOpts.XPaddingPlacement,
		XPaddingMethod:      xhttpOpts.XPaddingMethod,
		UplinkHTTPMethod:    xhttpOpts.UplinkHTTPMethod,
		SessionPlacement:    xhttpOpts.SessionPlacement,
		SessionKey:          xhttpOpts.SessionKey,
		SeqPlacement:        xhttpOpts.SeqPlacement,
		SeqKey:              xhttpOpts.SeqKey,
		UplinkDataPlacement: xhttpOpts.UplinkDataPlacement,
		UplinkDataKey:       xhttpOpts.UplinkDataKey,
		RequestLog:          requestLog,
		TryQUIC:             tryQuic,
	}
	if xhttpOpts.XPaddingBytesTo > 0 {
		config.XPaddingBytes = &splithttp.RangeConfig{From: xhttpOpts.XPaddingBytesFrom, To: xhttpOpts.XPaddingBytesTo}
	} else if compat := parseCompatXPaddingRange(xhttpOpts.XPaddingBytes); compat != nil {
		config.XPaddingBytes = compat
	}

	if config.Host == "" {
		config.Host = host
	}

	if splitHTTPOpts.Host != "" {
		config.Host = splitHTTPOpts.Host
	}
	if splitHTTPOpts.Path != "" {
		config.Path = splitHTTPOpts.Path
	}
	if splitHTTPOpts.MaxUploadSize > 0 {
		config.MaxUploadSize = splitHTTPOpts.MaxUploadSize
	}
	if splitHTTPOpts.MaxConcurrentPosts > 0 {
		config.MaxConcurrentPosts = splitHTTPOpts.MaxConcurrentPosts
	}
	if splitHTTPOpts.Mode != "" {
		config.Mode = splitHTTPOpts.Mode
	}
	if splitHTTPOpts.XPaddingBytesTo > 0 {
		config.XPaddingBytes = &splithttp.RangeConfig{From: splitHTTPOpts.XPaddingBytesFrom, To: splitHTTPOpts.XPaddingBytesTo}
	} else if compat := parseCompatXPaddingRange(splitHTTPOpts.XPaddingBytes); compat != nil {
		config.XPaddingBytes = compat
	}
	if splitHTTPOpts.XPaddingObfsMode {
		config.XPaddingObfsMode = true
	}
	if splitHTTPOpts.XPaddingKey != "" {
		config.XPaddingKey = splitHTTPOpts.XPaddingKey
	}
	if splitHTTPOpts.XPaddingHeader != "" {
		config.XPaddingHeader = splitHTTPOpts.XPaddingHeader
	}
	if splitHTTPOpts.XPaddingPlacement != "" {
		config.XPaddingPlacement = splitHTTPOpts.XPaddingPlacement
	}
	if splitHTTPOpts.XPaddingMethod != "" {
		config.XPaddingMethod = splitHTTPOpts.XPaddingMethod
	}
	if splitHTTPOpts.UplinkHTTPMethod != "" {
		config.UplinkHTTPMethod = splitHTTPOpts.UplinkHTTPMethod
	}
	if splitHTTPOpts.SessionPlacement != "" {
		config.SessionPlacement = splitHTTPOpts.SessionPlacement
	}
	if splitHTTPOpts.SessionKey != "" {
		config.SessionKey = splitHTTPOpts.SessionKey
	}
	if splitHTTPOpts.SeqPlacement != "" {
		config.SeqPlacement = splitHTTPOpts.SeqPlacement
	}
	if splitHTTPOpts.SeqKey != "" {
		config.SeqKey = splitHTTPOpts.SeqKey
	}
	if splitHTTPOpts.UplinkDataPlacement != "" {
		config.UplinkDataPlacement = splitHTTPOpts.UplinkDataPlacement
	}
	if splitHTTPOpts.UplinkDataKey != "" {
		config.UplinkDataKey = splitHTTPOpts.UplinkDataKey
	}

	for k, v := range xhttpOpts.Headers {
		config.Headers.Set(k, v)
	}
	for k, v := range splitHTTPOpts.Headers {
		config.Headers.Set(k, v)
	}
	config.ClientKey = buildSplitHTTPClientKey(config.DialAddr, config.TLSServerName, config.Host, config.ALPN, config.TLS)

	return config
}
