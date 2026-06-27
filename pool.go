// Package wpool provides a generic worker pool with backpressure, graceful
// shutdown, and result correlation.
package wpool

import (
	"context"
	"errors"
	"sync"
)

// Result carries the input that produced a unit of work alongside its
// outcome. On success Err is nil; on failure Out is the zero value and Err
// describes the failure. In is always populated so callers can correlate,
// log, or retry.
type Result[In, Out any] struct {
	In  In
	Out Out
	Err error
}

// Config configures a Pool.
type Config struct {
	// Workers is the number of worker goroutines. Must be > 0.
	Workers int
}

// Pool is a generic worker pool. Construct with New. Submit work with
// Publish. Consume outcomes from Results. Shut down with Close.
type Pool[In, Out any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers int
	convert func(context.Context, In) (Out, error)

	in  chan In
	out chan Result[In, Out]

	wg sync.WaitGroup

	closeMu sync.Mutex
	closed  bool
	sendWg  sync.WaitGroup
}

var (
	// ErrPoolClosed is returned by Publish after Close has been called.
	ErrPoolClosed = errors.New("wpool: pool is closed")
	// ErrNilConvert is returned by New when the convert function is nil.
	ErrNilConvert = errors.New("wpool: convert function is nil")
	// ErrInvalidWorkers is returned by New when Config.Workers <= 0.
	ErrInvalidWorkers = errors.New("wpool: workers must be > 0")
)

// New constructs a Pool. The pool's lifetime is bound to ctx: cancelling ctx
// stops workers (forceful). For graceful shutdown call Close.
//
// Callers MUST drain Results until it closes. A consumer that walks away
// will block workers mid-send and deadlock Close.
func New[In, Out any](
	ctx context.Context,
	cfg Config,
	convert func(context.Context, In) (Out, error),
) (*Pool[In, Out], error) {
	if convert == nil {
		return nil, ErrNilConvert
	}
	if cfg.Workers <= 0 {
		return nil, ErrInvalidWorkers
	}
	cctx, cancel := context.WithCancel(ctx)
	p := &Pool[In, Out]{
		ctx:     cctx,
		cancel:  cancel,
		workers: cfg.Workers,
		convert: convert,
		in:      make(chan In),
		out:     make(chan Result[In, Out]),
	}
	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go p.worker()
	}
	// Closer: outlives every worker so Results closes exactly once, whether
	// shutdown came from Close (graceful) or ctx cancel (forceful).
	go func() {
		p.wg.Wait()
		close(p.out)
	}()
	return p, nil
}

// Publish submits a job. Returns ErrPoolClosed if Close has been called or
// ctx.Err() if the pool's context is cancelled before the job is accepted.
func (p *Pool[In, Out]) Publish(job In) error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return ErrPoolClosed
	}
	p.sendWg.Add(1)
	p.closeMu.Unlock()
	defer p.sendWg.Done()

	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.in <- job:
		return nil
	}
}

// Results returns the channel of outcomes. Closed after Close returns, or
// after all workers exit due to context cancellation.
func (p *Pool[In, Out]) Results() <-chan Result[In, Out] {
	return p.out
}

// Close performs a graceful shutdown: refuses new Publish calls, waits for
// in-flight Publishes, closes the input so workers drain, waits for workers,
// then releases the context. Idempotent. Returns nil today; the error
// return is reserved for future timeout/force semantics.
func (p *Pool[In, Out]) Close() error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil
	}
	p.closed = true
	p.closeMu.Unlock()

	p.sendWg.Wait()
	close(p.in)
	p.wg.Wait()
	p.cancel()
	return nil
}

func (p *Pool[In, Out]) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.in:
			if !ok {
				return
			}
			out, err := p.convert(p.ctx, job)
			res := Result[In, Out]{In: job, Out: out, Err: err}
			select {
			case p.out <- res:
			case <-p.ctx.Done():
				return
			}
		}
	}
}
