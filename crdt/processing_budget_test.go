package crdt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestUnit_ApplyBudgetLargeDocumentAndPending(t *testing.T) {
	source := New()
	defer source.Destroy()
	m := source.GetMap("m")
	source.Transact(func(tx *Transaction) {
		for i := 0; i < 100000; i++ {
			m.Set(tx, fmt.Sprint(i), "value")
		}
	})
	raw := EncodeStateAsUpdateV1(source, nil)
	target := New()
	defer target.Destroy()
	budget := &ProcessingBudget{MaxWork: 100000000, MaxAllocatedBytes: 256 << 20, MaxValues: 8000000, MaxPayloadBytes: 64 << 20}
	if err := ApplyUpdateV1WithBudget(target, raw, nil, budget); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(EncodeStateAsUpdateV1(target, nil), raw) {
		t.Fatal("100k roundtrip changed")
	}
	usage, err := target.ResourceUsage(&ProcessingBudget{MaxWork: 1000000})
	if err != nil || usage.Items != 100000 || usage.RetainedBytes == 0 || budget.AllocatedBytes() == 0 {
		t.Fatalf("usage=%+v alloc=%d err=%v", usage, budget.AllocatedBytes(), err)
	}
	first := New()
	defer first.Destroy()
	mm := first.GetMap("m")
	first.Transact(func(tx *Transaction) { mm.Set(tx, "a", 1) })
	parent := EncodeStateAsUpdateV1(first, nil)
	sv, _ := DecodeStateVectorV1(EncodeStateVectorV1(first))
	first.Transact(func(tx *Transaction) { mm.Set(tx, "b", 2) })
	child := EncodeStateAsUpdateV1(first, sv)
	pending := New()
	defer pending.Destroy()
	if err = ApplyUpdateV1WithBudget(pending, child, nil, &ProcessingBudget{}); err != nil {
		t.Fatal(err)
	}
	before, err := pending.ResourceUsage(&ProcessingBudget{})
	if err != nil || before.PendingItems == 0 {
		t.Fatalf("pending usage=%+v err=%v", before, err)
	}
	if err = ApplyUpdateV1WithBudget(pending, parent, nil, &ProcessingBudget{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(EncodeStateAsUpdateV1(pending, nil), EncodeStateAsUpdateV1(first, nil)) {
		t.Fatal("pending resolution changed")
	}
}

func TestUnit_ApplyBudgetOwnsPayloadAndRecountsAfterSplitGC(t *testing.T) {
	source := New()
	defer source.Destroy()
	m := source.GetMap("m")
	text := source.GetText("text")
	source.Transact(func(tx *Transaction) {
		m.Set(tx, "binary", []byte{1, 2, 3, 4})
		text.Insert(tx, 0, strings.Repeat("a", 1<<20)+"z", nil)
	})
	target := New()
	defer target.Destroy()
	raw := EncodeStateAsUpdateV1(source, nil)
	if err := ApplyUpdateV1WithBudget(target, raw, nil, &ProcessingBudget{}); err != nil {
		t.Fatal(err)
	}
	clear(raw)
	value, _ := target.GetMap("m").Get("binary")
	if !bytes.Equal(value.([]byte), []byte{1, 2, 3, 4}) {
		t.Fatal("retained caller buffer")
	}
	before, err := target.ResourceUsage(&ProcessingBudget{})
	if err != nil {
		t.Fatal(err)
	}
	sv, _ := DecodeStateVectorV1(EncodeStateVectorV1(source))
	source.Transact(func(tx *Transaction) { text.Delete(tx, 0, 1<<20) })
	if err = ApplyUpdateV1WithBudget(target, EncodeStateAsUpdateV1(source, sv), nil, &ProcessingBudget{}); err != nil {
		t.Fatal(err)
	}
	after, err := target.ResourceUsage(&ProcessingBudget{})
	if err != nil || after.PayloadBytes >= before.PayloadBytes || target.GetText("text").ToString() != "z" {
		t.Fatalf("before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestUnit_ApplyBudgetRefusalAndObserverBoundary(t *testing.T) {
	source := New()
	defer source.Destroy()
	m := source.GetMap("m")
	source.Transact(func(tx *Transaction) {
		for i := 0; i < 100; i++ {
			m.Set(tx, fmt.Sprint(i), i)
		}
	})
	raw := EncodeStateAsUpdateV1(source, nil)
	for _, budget := range []*ProcessingBudget{{MaxWork: 1}, {MaxAllocatedBytes: 1}, {MaxValues: 1}, {MaxPayloadBytes: 1}} {
		target := New()
		err := ApplyUpdateV1WithBudget(target, raw, nil, budget)
		target.Destroy()
		if err == nil {
			t.Fatal("budget ignored")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	target := New()
	defer target.Destroy()
	if err := ApplyUpdateV1WithBudget(target, raw, nil, &ProcessingBudget{Context: ctx}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	fired := false
	target.OnUpdate(func([]byte, any) { fired = true })
	if err := ApplyUpdateV1WithBudget(target, raw, nil, &ProcessingBudget{}); !errors.Is(err, ErrBudgetObservers) || fired {
		t.Fatalf("err=%v fired=%v", err, fired)
	}
	if state := EncodeStateAsUpdateV1(target, nil); !bytes.Equal(state, []byte{0, 0}) {
		t.Fatal("known observer refusal mutated doc")
	}
}
