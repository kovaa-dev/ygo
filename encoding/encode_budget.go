package encoding

import (
	"context"
	"errors"
)

// ErrEncodeBudgetExceeded identifies an operational encoding refusal.
var ErrEncodeBudgetExceeded = errors.New("encoding: encode budget exceeded")

// EncodeBudget bounds output bytes and traversal work. Zero limits are unlimited.
// It is single-use and not safe for concurrent use. Reserve, when provided, runs
// before output-buffer capacity and known temporary-container allocations; the
// caller owns the lifetime of these cumulative allocation reservations.
// This is an allocation/work boundary, not an exact Go heap or RSS estimator.
type EncodeBudget struct {
	Context  context.Context
	MaxBytes uint64
	MaxWork  uint64
	Reserve  func(uint64) error
	work     uint64
}

// NewEncoderWithBudget creates an encoder with sticky budget errors and no
// initial allocation. Call Err before using Bytes. Existing encoders are unchanged.
func NewEncoderWithBudget(budget *EncodeBudget) *Encoder { return &Encoder{budget: budget} }

// Err returns the first operational error. Bytes must not be used after an error.
func (e *Encoder) Err() error { return e.err }

// Work charges traversal steps before processing a collection or payload.
func (e *Encoder) Work(n uint64) bool {
	if e.err != nil {
		return false
	}
	if e.budget == nil {
		return true
	}
	b := e.budget
	if b.Context != nil {
		if err := b.Context.Err(); err != nil {
			e.err = err
			return false
		}
	}
	if n > ^uint64(0)-b.work || (b.MaxWork > 0 && (b.work > b.MaxWork || n > b.MaxWork-b.work)) {
		e.err = ErrEncodeBudgetExceeded
		return false
	}
	b.work += n
	return true
}

func (e *Encoder) reserveTemporary(n uint64) bool {
	if e.budget != nil && e.budget.Reserve != nil {
		if err := e.budget.Reserve(n); err != nil {
			e.err = err
			return false
		}
	}
	return true
}

func (e *Encoder) reserve(n int) bool {
	if e.budget == nil {
		return true
	}
	if !e.Work(1) {
		return false
	}
	needed := uint64(len(e.buf)) + uint64(n)
	if needed < uint64(len(e.buf)) || (e.budget.MaxBytes > 0 && needed > e.budget.MaxBytes) {
		e.err = ErrEncodeBudgetExceeded
		return false
	}
	if needed <= uint64(cap(e.buf)) {
		return true
	}
	capacity := max(needed, uint64(cap(e.buf))*2, 64)
	if e.budget.MaxBytes > 0 && capacity > e.budget.MaxBytes {
		capacity = e.budget.MaxBytes
	}
	if capacity > uint64(int(^uint(0)>>1)) {
		e.err = ErrEncodeBudgetExceeded
		return false
	}
	if !e.reserveTemporary(capacity) {
		return false
	}
	next := make([]byte, len(e.buf), int(capacity))
	copy(next, e.buf)
	e.buf = next
	return true
}

// ReserveAllocation charges a known temporary allocation before materialization.
// Reservations are cumulative and are released by the caller of EncodeBudget.
func (e *Encoder) ReserveAllocation(n uint64) bool {
	return e.Work(0) && e.reserveTemporary(n)
}

// Budgeted reports whether this encoder has operational limits or reservations.
func (e *Encoder) Budgeted() bool { return e.budget != nil }

// Fail records an error from a content-specific bounded preflight.
func (e *Encoder) Fail(err error) {
	if e.err == nil {
		e.err = err
	}
}
