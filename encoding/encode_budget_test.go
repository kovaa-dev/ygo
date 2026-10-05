package encoding

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestUnit_EncoderBudgetRefusesBeforeAllocation(t *testing.T) {
	var reserved uint64
	e := NewEncoderWithBudget(&EncodeBudget{MaxBytes: 8, Reserve: func(n uint64) error { reserved += n; return nil }})
	e.WriteRaw(make([]byte, 9))
	if !errors.Is(e.Err(), ErrEncodeBudgetExceeded) || reserved != 0 || len(e.Bytes()) != 0 {
		t.Fatalf("err=%v reserved=%d bytes=%d", e.Err(), reserved, len(e.Bytes()))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e = NewEncoderWithBudget(&EncodeBudget{Context: ctx})
	e.WriteUint8(1)
	if !errors.Is(e.Err(), context.Canceled) {
		t.Fatal(e.Err())
	}
}
func TestUnit_EncoderBudgetExactLimitAndStickyFailure(t *testing.T) {
	old := NewEncoder()
	old.WriteAny(map[string]any{"a": []any{int64(-128), "hello", []byte{1, 2}, true}, "b": float64(3.5)})
	e := NewEncoderWithBudget(&EncodeBudget{MaxBytes: uint64(len(old.Bytes())), MaxWork: 10000})
	e.WriteAny(map[string]any{"a": []any{int64(-128), "hello", []byte{1, 2}, true}, "b": float64(3.5)})
	if e.Err() != nil || !bytes.Equal(e.Bytes(), old.Bytes()) {
		t.Fatalf("wire changed err=%v", e.Err())
	}
	e.WriteUint8(0)
	if !errors.Is(e.Err(), ErrEncodeBudgetExceeded) {
		t.Fatal(e.Err())
	}
	e.WriteAny(make([]any, 1000))
	if len(e.Bytes()) != len(old.Bytes()) {
		t.Fatal("continued after refusal")
	}
}
