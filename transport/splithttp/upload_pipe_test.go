package splithttp

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestUploadPipeReadSplit(t *testing.T) {
	pipe := newUploadPipe(32)
	if _, err := pipe.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	first, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("first Read failed: %v", err)
	}
	if string(first) != "abcd" {
		t.Fatalf("first Read = %q, want %q", string(first), "abcd")
	}

	second, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("second Read failed: %v", err)
	}
	if string(second) != "ef" {
		t.Fatalf("second Read = %q, want %q", string(second), "ef")
	}
}

func TestUploadPipeInterrupt(t *testing.T) {
	pipe := newUploadPipe(32)
	wantErr := errors.New("upload failed")
	done := make(chan error, 1)

	go func() {
		_, err := pipe.ReadChunk(4)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	pipe.Interrupt(wantErr)

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Read error = %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interrupted Read")
	}
}

func TestUploadPipeCloseEOF(t *testing.T) {
	pipe := newUploadPipe(32)
	done := make(chan error, 1)

	go func() {
		_, err := pipe.ReadChunk(4)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	_ = pipe.Close()

	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed Read")
	}
}

func TestUploadPipeBackpressure(t *testing.T) {
	pipe := newUploadPipe(4)
	if _, err := pipe.Write([]byte("abcd")); err != nil {
		t.Fatalf("initial Write failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := pipe.Write([]byte("ef"))
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("Write returned early with %v; expected blocking backpressure", err)
	case <-time.After(50 * time.Millisecond):
	}

	chunk, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	if string(chunk) != "abcd" {
		t.Fatalf("ReadChunk = %q, want %q", string(chunk), "abcd")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked Write failed after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked Write to resume")
	}
}
