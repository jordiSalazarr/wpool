package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jordiSalazarr/wpool"
)

type Job struct {
	Id   int
	Name string
}

type JobResult struct {
	val string
}

var testFunc = func(ctx context.Context, in Job) (JobResult, error) {
	return JobResult{val: fmt.Sprintf("%v result %s", in.Id, in.Name)}, nil
}

func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		workers     int
		convert     func(context.Context, Job) (JobResult, error)
		expectError bool
	}{
		{name: "0 workers errors", workers: 0, convert: testFunc, expectError: true},
		{name: "negative workers errors", workers: -10, convert: testFunc, expectError: true},
		{name: "nil convert errors", workers: 10, convert: nil, expectError: true},
		{name: "100 workers ok", workers: 100, convert: testFunc, expectError: false},
		{name: "50 workers ok", workers: 50, convert: testFunc, expectError: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := New(context.Background(), wpool.Config{Workers: tt.workers}, tt.convert)
			if (err != nil) != tt.expectError {
				t.Fatalf("expectError=%v, got err=%v", tt.expectError, err)
			}
			if err == nil {
				_ = p.Close()
			}
		})
	}
}

func TestPoolContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p, err := New(ctx, wpool.Config{Workers: 4}, testFunc)
	if err != nil {
		t.Fatal(err)
	}

	drained := make(chan struct{})
	go func() {
		for range p.Results() {
		}
		close(drained)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("Results never closed after ctx cancel")
	}
}

func TestPublishAndReceive(t *testing.T) {
	total := 10
	p, err := New(context.Background(), wpool.Config{Workers: 100}, testFunc)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := range total {
			if err := p.Publish(Job{Id: i, Name: "x"}); err != nil {
				t.Errorf("publish %d: %v", i, err)
				return
			}
		}
		_ = p.Close()
	}()

	received := 0
	for r := range p.Results() {
		if r.Err != nil {
			t.Errorf("unexpected err for job %d: %v", r.In.Id, r.Err)
		}
		received++
	}

	if received != total {
		t.Errorf("sent=%d received=%d", total, received)
	}
}

func TestPublishAfterCloseReturnsError(t *testing.T) {
	p, err := New(context.Background(), wpool.Config{Workers: 1}, testFunc)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range p.Results() {
		}
	}()
	_ = p.Close()

	if err := p.Publish(Job{}); !errors.Is(err, wpool.ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestResultCarriesInputOnError(t *testing.T) {
	failing := func(ctx context.Context, in Job) (JobResult, error) {
		return JobResult{}, fmt.Errorf("boom %d", in.Id)
	}
	p, err := New(context.Background(), wpool.Config{Workers: 2}, failing)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		_ = p.Publish(Job{Id: 42, Name: "x"})
		_ = p.Close()
	}()

	var seen bool
	for r := range p.Results() {
		if r.Err == nil {
			t.Errorf("expected error for job %d", r.In.Id)
			continue
		}
		if r.In.Id == 42 {
			seen = true
		}
	}
	if !seen {
		t.Errorf("did not see result correlated to input id 42")
	}
}

func TestPublishOnClosedPool(t *testing.T) {
	pool, err := New(context.Background(), wpool.Config{Workers: 100}, testFunc)
	if err != nil {
		t.Errorf("error creating pool")
	}
	pool.Close()
	err = pool.Publish(Job{Id: 1111, Name: "dfwdnwd"})

	if err == nil {
		t.Errorf("publishing on a closed pool should error")
	}
}

func TestClosingTwice(t *testing.T) {
	pool, err := New(context.Background(), wpool.Config{Workers: 100}, testFunc)
	if err != nil {
		t.Errorf("error creating pool")
	}
	_ = pool.Close()
	err = pool.Close()
	if err != nil {
		t.Errorf("closing twice should be idempotent")
	}
}
