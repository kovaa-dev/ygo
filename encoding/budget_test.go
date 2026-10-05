package encoding

import (
	"context"
	"errors"
	"testing"
)

func TestUnit_DecodeBudget_RefusesBeforeContainer(t *testing.T) {
	enc := NewEncoder()
	enc.WriteUint8(117)
	enc.WriteVarUint(100000)
	enc.WriteRaw(make([]byte, 100000))
	dec := NewDecoderWithBudget(enc.Bytes(), &DecodeBudget{MaxValues: 10})
	if _, err := dec.ReadAny(); !errors.Is(err, ErrDecodeBudgetExceeded) {
		t.Fatalf("budget: %v", err)
	}
	if dec.Remaining() != 100000 {
		t.Fatal("decoded container after refusal")
	}
}
func TestUnit_DecodeBudget_PayloadAndCancellation(t *testing.T) {
	enc := NewEncoder()
	enc.WriteVarString("hello")
	dec := NewDecoderWithBudget(enc.Bytes(), &DecodeBudget{MaxPayloadBytes: 4})
	if _, err := dec.ReadVarString(); !errors.Is(err, ErrDecodeBudgetExceeded) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dec = NewDecoderWithBudget(enc.Bytes(), &DecodeBudget{Context: ctx})
	if _, err := dec.ReadVarString(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := dec.ReservePayload(1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := dec.ReserveValues(1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
