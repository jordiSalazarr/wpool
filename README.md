# wpool

Generic worker pool for Go (1.21+) with backpressure, graceful shutdown, and result-to-input correlation.

Two variants live in subpackages — pick the one that fits your call site:

| Subpackage | Result delivery | Use when |
|---|---|---|
| [`wpool/future`](./future) | Each `Publish` returns its own `<-chan Result` | Request/response — e.g. an HTTP handler that calls an external API and needs *its* answer back |
| [`wpool/stream`](./stream) | All workers send to one shared `Results()` channel | Pipeline/batch — one producer fans out N jobs, one consumer drains everything |

Shared types (`Result`, `Config`, `Err*`) live in the root `wpool` package so you can swap variants without rewriting your job/result types.

## Install

```
go get github.com/jordiSalazarr/wpool
```

## `future` — per-job result channel

Each `Publish` hands you back a buffered channel that receives exactly one `Result`. Lets each caller wait on *their* job without correlation bookkeeping.

```go
package main

import (
    "context"
    "fmt"
    "net/http"

    "github.com/jordiSalazarr/wpool"
    "github.com/jordiSalazarr/wpool/future"
)

type ChargeReq struct{ CustomerID string }
type ChargeRes struct{ ID string }

func main() {
    callStripe := func(ctx context.Context, req ChargeReq) (ChargeRes, error) {
        // ... HTTP call to Stripe ...
        return ChargeRes{ID: "ch_123"}, nil
    }

    pool, _ := future.New(context.Background(), wpool.Config{Workers: 10}, callStripe)
    defer pool.Close()

    http.HandleFunc("/charge", func(w http.ResponseWriter, r *http.Request) {
        ch, err := pool.Publish(ChargeReq{CustomerID: "cus_42"})
        if err != nil {
            http.Error(w, err.Error(), http.StatusServiceUnavailable)
            return
        }
        select {
        case res := <-ch:
            if res.Err != nil {
                http.Error(w, res.Err.Error(), http.StatusBadGateway)
                return
            }
            fmt.Fprintf(w, "charged: %s\n", res.Out.ID)
        case <-r.Context().Done():
            return // client disconnected
        }
    })

    _ = http.ListenAndServe(":8080", nil)
}
```

`Workers` caps concurrency against the downstream API regardless of traffic — extra `Publish` calls block until a worker frees up.

## `stream` — shared results channel

One channel for everything. Idiomatic Go pipeline.

```go
package main

import (
    "context"
    "fmt"

    "github.com/jordiSalazarr/wpool"
    "github.com/jordiSalazarr/wpool/stream"
)

type Job struct{ ID int }
type Out struct{ Doubled int }

func main() {
    convert := func(ctx context.Context, j Job) (Out, error) {
        return Out{Doubled: j.ID * 2}, nil
    }

    p, err := stream.New(context.Background(), wpool.Config{Workers: 8}, convert)
    if err != nil {
        panic(err)
    }

    go func() {
        for i := 0; i < 100; i++ {
            _ = p.Publish(Job{ID: i})
        }
        _ = p.Close()
    }()

    for r := range p.Results() {
        if r.Err != nil {
            fmt.Printf("job %d failed: %v\n", r.In.ID, r.Err)
            continue
        }
        fmt.Printf("job %d -> %d\n", r.In.ID, r.Out.Doubled)
    }
}
```

## Semantics (both variants)

- **Backpressure.** Input is unbuffered. Publishers block until a worker is ready.
- **Result correlation.** Every `Result` carries the original input, so failures can be retried or logged without external bookkeeping.
- **Graceful shutdown via `Close`.** Refuses new `Publish` calls, drains in-flight publishes, lets workers finish.
- **Forceful shutdown via `ctx`.** Cancelling the constructor `ctx` unblocks workers and tears the pool down.
- **Idempotent `Close`.**
- **`stream` consumer contract.** Callers MUST drain `Results()` until it closes — a consumer that walks away will deadlock `Close`. (`future` has no such constraint: each result channel is buffered.)

## Errors

- `wpool.ErrInvalidWorkers` — `Config.Workers <= 0`
- `wpool.ErrNilConvert` — `convert` is `nil`
- `wpool.ErrPoolClosed` — `Publish` called after `Close`

## License

MIT
