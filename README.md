# wpool

A small, generic worker pool for Go (1.21+) with backpressure, graceful shutdown, and result-to-input correlation.

## Install

```
go get github.com/jordiSalazarr/wpool
```

## Use

```go
package main

import (
    "context"
    "fmt"

    "github.com/jordiSalazarr/wpool"
)

type Job struct{ ID int }
type Out struct{ Doubled int }

func main() {
    ctx := context.Background()

    convert := func(ctx context.Context, j Job) (Out, error) {
        return Out{Doubled: j.ID * 2}, nil
    }

    p, err := wpool.New(ctx, wpool.Config{Workers: 8}, convert)
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

## Semantics

- **Backpressure.** `in` and `out` are unbuffered. Publishers block until a worker is ready; workers block on send until the consumer reads.
- **Result correlation.** Every `Result` carries the original input, so failures can be retried, logged, or batched without external bookkeeping.
- **Graceful shutdown via `Close`.** Refuses new `Publish` calls, drains in-flight publishes, lets workers finish, then closes `Results`.
- **Forceful shutdown via `ctx`.** Cancelling the constructor `ctx` unblocks workers mid-send and closes `Results`. Pending `Publish` calls return `ctx.Err()`.
- **Idempotent `Close`.**
- **Required contract.** Callers MUST drain `Results` until it closes — a consumer that walks away will deadlock `Close`.

## Errors

- `ErrInvalidWorkers` — `Config.Workers <= 0`
- `ErrNilConvert` — `convert` is `nil`
- `ErrPoolClosed` — `Publish` called after `Close`

## License

MIT
