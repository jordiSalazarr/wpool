// Package stream provides a worker pool with a single shared results
// channel. Use it for pipeline/batch processing with one (or a known small
// set of) consumer(s).
package stream

import (
	"context"
	"sync"

	"github.com/jordiSalazarr/wpool"
)

// Pool is a worker pool whose workers multiplex their results into a single
// channel exposed by Results. Construct with New. Submit work with Publish.
// Shut down with Close.
//
// Callers MUST drain Results until it closes. A consumer that walks away
// will block workers mid-send and deadlock Close.
type Pool[In, Out any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers int
	convert func(context.Context, In) (Out, error)

	in  chan In
	out chan wpool.Result[In, Out]

	wg sync.WaitGroup

	closeMu sync.Mutex
	closed  bool
	sendWg  sync.WaitGroup
}

// New constructs a Pool. The pool's lifetime is bound to ctx: cancelling ctx
// stops workers (forceful). For graceful shutdown call Close.
func New[In, Out any](
	ctx context.Context,
	cfg wpool.Config,
	convert func(context.Context, In) (Out, error),
) (*Pool[In, Out], error) {
	if convert == nil {
		return nil, wpool.ErrNilConvert
	}
	if cfg.Workers <= 0 {
		return nil, wpool.ErrInvalidWorkers
	}
	cctx, cancel := context.WithCancel(ctx)
	p := &Pool[In, Out]{
		ctx:     cctx,
		cancel:  cancel,
		workers: cfg.Workers,
		convert: convert,
		in:      make(chan In),
		out:     make(chan wpool.Result[In, Out]),
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
		return wpool.ErrPoolClosed
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
func (p *Pool[In, Out]) Results() <-chan wpool.Result[In, Out] {
	return p.out
}

// Close performs a graceful shutdown: refuses new Publish calls, waits for
// in-flight Publishes, closes the input so workers drain, waits for workers,
// then releases the context. Idempotent.
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
			res := wpool.Result[In, Out]{In: job, Out: out, Err: err}
			select {
			case p.out <- res:
			case <-p.ctx.Done():
				return
			}
		}
	}
}
