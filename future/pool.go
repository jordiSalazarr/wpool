// Package future provides a worker pool where each Publish returns a
// per-job result channel. Use it for request/response style work
// (e.g. HTTP handlers calling an external API that need their own answer).
package future

import (
	"context"
	"sync"

	"github.com/jordiSalazarr/wpool"
)

type job[In, Out any] struct {
	in           In
	resultStream chan<- wpool.Result[In, Out]
}

// Pool is a worker pool where each submitted job gets its own result channel.
// Construct with New. Submit work with Publish. Shut down with Close.
type Pool[In, Out any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers int
	convert func(context.Context, In) (Out, error)

	in chan job[In, Out]

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
		in:      make(chan job[In, Out]),
	}
	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go p.worker()
	}
	return p, nil
}

// Publish submits a job and returns a buffered channel that will receive
// exactly one Result. Returns ErrPoolClosed if Close has been called or
// ctx.Err() if the pool's context is cancelled before the job is accepted.
func (p *Pool[In, Out]) Publish(in In) (<-chan wpool.Result[In, Out], error) {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil, wpool.ErrPoolClosed
	}
	resultChan := make(chan wpool.Result[In, Out], 1)
	p.sendWg.Add(1)
	p.closeMu.Unlock()
	defer p.sendWg.Done()

	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case p.in <- job[In, Out]{in: in, resultStream: resultChan}:
		return resultChan, nil
	}
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
		case j, ok := <-p.in:
			if !ok {
				return
			}
			out, err := p.convert(p.ctx, j.in)
			res := wpool.Result[In, Out]{In: j.in, Out: out, Err: err}
			select {
			case j.resultStream <- res:
			case <-p.ctx.Done():
				j.resultStream <- wpool.Result[In, Out]{In: j.in, Out: out, Err: p.ctx.Err()}
				return
			}
		}
	}
}
