package encoding

import (
	"bytes"
	"errors"
)

// ErrEncodingMismatch means the emitted bytes differ from the expected encoding.
var ErrEncodingMismatch = errors.New("encoding: output differs from expected bytes")

type comparison struct {
	expected []byte
	offset   int
}

// NewComparingEncoder is single-use and compares output without retaining it. The
// caller must keep expected immutable until FinishComparison returns. Bytes is
// not an output API for this encoder. Output/work limits remain enforced.
func NewComparingEncoder(expected []byte, budget *EncodeBudget) *Encoder {
	return &Encoder{budget: budget, comparison: &comparison{expected: expected}}
}

// FinishComparison flushes scalar scratch bytes and checks the complete length.
func (e *Encoder) FinishComparison() error {
	if e.comparison == nil {
		return errors.New("encoding: not a comparing encoder")
	}
	e.flushComparison()
	if e.err == nil && e.comparison.offset != len(e.comparison.expected) {
		e.err = ErrEncodingMismatch
	}
	return e.err
}

func (e *Encoder) comparisonLimit(n int) bool {
	if !e.Work(1) {
		return false
	}
	used := uint64(e.comparison.offset) + uint64(len(e.buf))
	needed := used + uint64(n)
	if n < 0 || needed < used || (e.budget != nil && e.budget.MaxBytes > 0 && needed > e.budget.MaxBytes) {
		e.err = ErrEncodeBudgetExceeded
		return false
	}
	if needed > uint64(len(e.comparison.expected)) {
		e.err = ErrEncodingMismatch
		return false
	}
	return true
}

func (e *Encoder) flushComparison() {
	if e.err != nil || len(e.buf) == 0 {
		return
	}
	c := e.comparison
	if !bytes.Equal(e.buf, c.expected[c.offset:c.offset+len(e.buf)]) {
		e.err = ErrEncodingMismatch
		return
	}
	c.offset += len(e.buf)
	e.buf = e.buf[:0]
}

func (e *Encoder) reserveComparison(n int) bool {
	if !e.comparisonLimit(n) {
		return false
	}
	if len(e.buf)+n <= cap(e.buf) {
		return true
	}
	e.flushComparison()
	if e.err != nil {
		return false
	}
	if n > cap(e.buf) {
		size := max(64, n)
		if !e.reserveTemporary(uint64(size)) {
			return false
		}
		e.buf = make([]byte, 0, size)
	}
	return true
}

func (e *Encoder) compareBytes(b []byte) {
	if !e.comparisonLimit(len(b)) {
		return
	}
	e.flushComparison()
	if e.err != nil {
		return
	}
	c := e.comparison
	if !bytes.Equal(b, c.expected[c.offset:c.offset+len(b)]) {
		e.err = ErrEncodingMismatch
		return
	}
	c.offset += len(b)
}
func (e *Encoder) compareString(s string) {
	if !e.comparisonLimit(len(s)) {
		return
	}
	e.flushComparison()
	if e.err != nil {
		return
	}
	c := e.comparison
	if s != string(c.expected[c.offset:c.offset+len(s)]) {
		e.err = ErrEncodingMismatch
		return
	}
	c.offset += len(s)
}
