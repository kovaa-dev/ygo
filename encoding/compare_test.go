package encoding

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestComparingEncoderExactBytesAndBounds(t *testing.T) {
	write := func(e *Encoder) {
		e.WriteVarUint(1 << 52)
		e.WriteVarInt(-1234567)
		e.WriteFloat32(1.25)
		e.WriteFloat64(-3.5)
		e.WriteBigInt64(-1 << 60)
		e.WriteVarString(strings.Repeat("🦊", 100000))
		e.WriteVarBytes(bytes.Repeat([]byte{1, 2, 3}, 100000))
		e.WriteAny(map[string]any{"nested": []any{int64(42), nil, true, false, float32(2), "text"}})
		e.WriteRaw([]byte{0, 1, 2})
	}
	normal := NewEncoder()
	write(normal)
	expected := bytes.Clone(normal.Bytes())
	var reserved uint64
	comparing := NewComparingEncoder(expected, &EncodeBudget{MaxBytes: uint64(len(expected)), MaxWork: 10000000, Reserve: func(n uint64) error { reserved += n; return nil }})
	write(comparing)
	if err := comparing.FinishComparison(); err != nil {
		t.Fatal(err)
	}
	if reserved > 4096 {
		t.Fatalf("retained output-sized storage: %d", reserved)
	}
	if !bytes.Equal(expected, normal.Bytes()) {
		t.Fatal("changed expected bytes")
	}
	for _, index := range []int{0, len(expected) / 2, len(expected) - 1} {
		changed := bytes.Clone(expected)
		changed[index] ^= 1
		enc := NewComparingEncoder(changed, nil)
		write(enc)
		if !errors.Is(enc.FinishComparison(), ErrEncodingMismatch) {
			t.Fatalf("accepted corruption at %d", index)
		}
	}
	for _, data := range [][]byte{nil, expected[:len(expected)-1], append(bytes.Clone(expected), 0)} {
		enc := NewComparingEncoder(data, nil)
		write(enc)
		if !errors.Is(enc.FinishComparison(), ErrEncodingMismatch) {
			t.Fatal("accepted length mismatch")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, budget := range []*EncodeBudget{{MaxBytes: uint64(len(expected) - 1)}, {MaxWork: 1}, {Context: ctx}, {Reserve: func(uint64) error { return ErrEncodeBudgetExceeded }}} {
		enc := NewComparingEncoder(expected, budget)
		write(enc)
		if err := enc.FinishComparison(); !errors.Is(err, ErrEncodeBudgetExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("budget ignored: %v", err)
		}
	}
	if err := NewComparingEncoder(nil, nil).FinishComparison(); err != nil {
		t.Fatal(err)
	}
}

func FuzzComparingEncoder(f *testing.F) {
	f.Add("hello 🦊", []byte{1, 2, 3}, int64(-123), uint64(123))
	f.Fuzz(func(t *testing.T, s string, b []byte, n int64, u uint64) {
		// Strings need valid UTF-8; raw byte payloads cover all byte values.
		s = strings.ToValidUTF8(s, "�")
		n %= 1 << 54
		u %= 1 << 53
		write := func(e *Encoder) { e.WriteVarString(s); e.WriteVarBytes(b); e.WriteVarInt(n); e.WriteVarUint(u) }
		normal := NewEncoder()
		write(normal)
		enc := NewComparingEncoder(normal.Bytes(), &EncodeBudget{MaxBytes: uint64(len(normal.Bytes()))})
		write(enc)
		if err := enc.FinishComparison(); err != nil {
			t.Fatal(err)
		}
	})
}
