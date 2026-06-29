package future

import (
	"context"
	"fmt"
	"testing"

	"github.com/jordiSalazarr/wpool"
)

type JobT struct {
	Id   int
	Name string
}

type JobResult struct {
	val string
}

var testFunc = func(ctx context.Context, in JobT) (JobResult, error) {
	return JobResult{val: fmt.Sprintf("%v result %s", in.Id, in.Name)}, nil
}

func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		workers     int
		convert     func(context.Context, JobT) (JobResult, error)
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

func TestResultCarriesInputOnError(t *testing.T) {
	failing := func(ctx context.Context, in JobT) (JobResult, error) {
		return JobResult{}, fmt.Errorf("boom %d", in.Id)
	}
	p, err := New(context.Background(), wpool.Config{Workers: 2}, failing)
	if err != nil {
		t.Fatal(err)
	}

	resultStream, err := p.Publish(JobT{Id: 42, Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = p.Close() }()

	res := <-resultStream
	if res.In.Id != 42 || res.In.Name != "x" {
		t.Errorf("input not preserved: got %v", res.In)
	}
	if res.Err == nil {
		t.Errorf("expected error, got nil")
	}
}
