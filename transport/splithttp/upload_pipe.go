package splithttp

import (
	"io"
	"sync"
)

type uploadBatchReader interface {
	ReadChunk(maxBytes int) ([]byte, error)
}

type uploadBatchWriter interface {
	Write([]byte) (int, error)
	Close() error
	Interrupt(error)
}

type uploadPipeline interface {
	uploadBatchReader
	uploadBatchWriter
}

type uploadPipe struct {
	mu          sync.Mutex
	cond        *sync.Cond
	queue       [][]byte
	buffered    int
	limit       int
	closed      bool
	interrupted bool
	err         error
}

func newUploadPipe(limit int) *uploadPipe {
	p := &uploadPipe{limit: limit}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *uploadPipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if p.err != nil {
			return 0, p.err
		}
		if p.closed {
			return 0, io.ErrClosedPipe
		}
		if p.limit <= 0 || p.buffered+len(b) <= p.limit {
			break
		}
		p.cond.Wait()
	}

	copied := append([]byte(nil), b...)
	p.queue = append(p.queue, copied)
	p.buffered += len(copied)
	p.cond.Signal()
	return len(b), nil
}

func (p *uploadPipe) ReadChunk(maxBytes int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if len(p.queue) > 0 {
			break
		}
		if p.err != nil {
			return nil, p.err
		}
		if p.interrupted {
			return nil, io.ErrClosedPipe
		}
		if p.closed {
			return nil, io.EOF
		}
		p.cond.Wait()
	}

	if maxBytes <= 0 {
		maxBytes = p.buffered
	}

	out := make([]byte, 0, minInt(maxBytes, p.buffered))
	for len(p.queue) > 0 && len(out) < maxBytes {
		head := p.queue[0]
		need := maxBytes - len(out)
		if len(head) <= need {
			out = append(out, head...)
			p.queue = p.queue[1:]
			p.buffered -= len(head)
			continue
		}
		out = append(out, head[:need]...)
		p.queue[0] = append([]byte(nil), head[need:]...)
		p.buffered -= need
	}
	p.cond.Broadcast()
	return out, nil
}

func (p *uploadPipe) Interrupt(err error) {
	p.mu.Lock()
	if err == nil {
		err = io.ErrClosedPipe
	}
	if p.err == nil {
		p.err = err
	}
	p.interrupted = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *uploadPipe) Close() error {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
