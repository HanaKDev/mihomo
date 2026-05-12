package splithttp

import (
	"io"
	"net"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
)

type managedConn struct {
	id         uint64
	sessionID  string
	mode       string
	config     *SplitHTTPConfig
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()
	closeOnce  sync.Once
}

func (c *managedConn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

func (c *managedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *managedConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.writer.Close()
		err2 := c.reader.Close()
		if err == nil {
			err = err2
		}
		if c.onClose != nil {
			c.onClose()
		}
		active := splitHTTPDiagActiveConns.Add(-1)
		splitHTTPDiagLog(c.config, "managed-conn close id=%d session=%s mode=%s active=%d", c.id, c.sessionID, c.mode, active)
	})
	return err
}

func (c *managedConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *managedConn) RemoteAddr() net.Addr { return c.remoteAddr }
func (c *managedConn) SetDeadline(time.Time) error {
	return nil
}
func (c *managedConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *managedConn) SetWriteDeadline(time.Time) error {
	return nil
}

func IsManagedConn(conn net.Conn) bool {
	for {
		switch conn.(type) {
		case *managedConn, *splitConn:
			return true
		}
		upstream, ok := conn.(N.WithUpstream)
		if !ok {
			return false
		}
		next, ok := upstream.Upstream().(net.Conn)
		if !ok || next == conn {
			return false
		}
		conn = next
	}
}
