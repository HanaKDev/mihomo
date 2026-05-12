package vless

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

type recordingConn struct {
	writes [][]byte
}

func (c *recordingConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *recordingConn) Close() error                     { return nil }
func (c *recordingConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *recordingConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *recordingConn) Write(b []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), b...))
	return len(b), nil
}

func TestDialVisionXUDPPacketConnSendsVisionBodyAfterMuxHeader(t *testing.T) {
	const uuidString = "00000000-0000-0000-0000-000000000001"
	client, err := NewClient(uuidString, &Addons{Flow: XRV})
	if err != nil {
		t.Fatal(err)
	}
	conn := &recordingConn{}
	pc, err := client.DialVisionXUDPPacketConn(conn, [8]byte{}, &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443}, &DstAddr{Mux: true, UDP: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected VLESS request header before XUDP body, got %d writes", len(conn.writes))
	}

	header := conn.writes[0]
	if len(header) < 19 {
		t.Fatalf("VLESS request header too short: %d", len(header))
	}
	if header[0] != Version {
		t.Fatalf("unexpected VLESS version: %d", header[0])
	}
	addonsLen := int(header[17])
	if addonsLen == 0 {
		t.Fatal("expected XRV addons in VLESS request header")
	}
	commandOffset := 18 + addonsLen
	if len(header) <= commandOffset || header[commandOffset] != CommandMux {
		t.Fatalf("expected CommandMux after addons, header=%x", header)
	}

	if _, err := pc.WriteTo([]byte{1, 2, 3}, &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443}); err != nil {
		t.Fatal(err)
	}
	if len(conn.writes) != 2 {
		t.Fatalf("expected first XUDP packet to write Vision body, got %d writes", len(conn.writes))
	}
	uid := uuid.FromStringOrNil(uuidString)
	if !bytes.Equal(conn.writes[1][:uuid.Size], uid.Bytes()) {
		t.Fatalf("expected Vision body padding to start with user UUID, got %x", conn.writes[1][:uuid.Size])
	}
}
