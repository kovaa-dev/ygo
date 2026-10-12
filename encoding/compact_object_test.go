package encoding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestCompactObjectWireJSONAndDuplicateKeys(t *testing.T) {
	e := NewEncoder()
	e.WriteUint8(118)
	e.WriteVarUint(3)
	e.WriteVarString("z")
	e.WriteAny("old")
	e.WriteVarString("a")
	e.WriteAny(map[string]any{"<nested>": []any{int64(3), true, nil, "\u2028\n", []byte{1, 2}}})
	e.WriteVarString("z")
	e.WriteAny("new")
	plain, err := NewDecoder(e.Bytes()).ReadAny()
	if err != nil {
		t.Fatal(err)
	}
	compact, err := NewDecoderWithBudget(e.Bytes(), &DecodeBudget{CompactObjects: true, CopyPayload: true}).ReadAny()
	if err != nil {
		t.Fatal(err)
	}
	o, ok := compact.(*Object)
	if !ok || o.Len() != 2 {
		t.Fatalf("%T %v", compact, compact)
	}
	if v, ok := o.Get("z"); !ok || v != "new" {
		t.Fatal(v, ok)
	}
	left, right := NewEncoder(), NewEncoder()
	left.WriteAny(plain)
	right.WriteAny(compact)
	if !bytes.Equal(left.Bytes(), right.Bytes()) {
		t.Fatal("wire differs")
	}
	a, _ := json.Marshal(plain)
	b, err := json.Marshal(compact)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("JSON differs: %s %s %v", a, b, err)
	}
	round, err := NewDecoder(right.Bytes()).ReadAny()
	if err != nil || !reflect.DeepEqual(plain, round) {
		t.Fatal("roundtrip", err)
	}
}

func TestCompactObjectBudgetRefusal(t *testing.T) {
	e := NewEncoder()
	e.WriteAny(map[string]any{"a": map[string]any{"b": "c"}})
	sentinel := errors.New("refuse")
	for _, b := range []*DecodeBudget{
		{CompactObjects: true, MaxValues: 1},
		{CompactObjects: true, Reserve: func(uint64) error { return sentinel }},
		{CompactObjects: true, Work: func(uint64) error { return sentinel }},
	} {
		if _, err := NewDecoderWithBudget(e.Bytes(), b).ReadAny(); err == nil {
			t.Fatal("accepted refused allocation/work")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewDecoderWithBudget(e.Bytes(), &DecodeBudget{CompactObjects: true, Context: ctx}).ReadAny(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
