// Package wpool provides shared types for the worker pool variants.
//
// Two implementations live in subpackages:
//
//   - future: each Publish returns a per-job result channel. Use it for
//     request/response style work (e.g. HTTP handlers that need their own
//     answer).
//   - stream: a single shared results channel for all workers. Use it for
//     pipeline/batch processing with one consumer.
//
// Both share the Result, Config, and error types defined here so callers can
// switch variants without rewriting their job and result types.
package wpool

import "errors"

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

var (
	// ErrPoolClosed is returned by Publish after Close has been called.
	ErrPoolClosed = errors.New("wpool: pool is closed")
	// ErrNilConvert is returned when the convert function is nil.
	ErrNilConvert = errors.New("wpool: convert function is nil")
	// ErrInvalidWorkers is returned when Config.Workers <= 0.
	ErrInvalidWorkers = errors.New("wpool: workers must be > 0")
)
