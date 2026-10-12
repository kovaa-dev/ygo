package crdt

import (
	"context"
	"errors"
	"testing"
)

func TestChangedKeysNestedAndRefusal(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	out := map[string]map[string]bool{}
	overflow := false
	text := NewTextPrelim()
	if err := doc.TransactWithBudget(func(tx *Transaction) error { tx.GetMap("contents").Set(tx, "key", text); return nil }, &ProcessingBudget{ChangedKeys: out, ChangedKeysLimit: 256 << 10, ChangedKeysOverflow: &overflow}); err != nil {
		t.Fatal(err)
	}
	if !out["contents"]["key"] {
		t.Fatal("insert not tracked")
	}
	clear(out)
	if err := doc.TransactWithBudget(func(tx *Transaction) error { text.Insert(tx, 0, "abc", nil); return nil }, &ProcessingBudget{ChangedKeys: out, ChangedKeysLimit: 256 << 10, ChangedKeysOverflow: &overflow}); err != nil {
		t.Fatal(err)
	}
	if !out["contents"]["key"] {
		t.Fatal("nested edit not tracked")
	}
	clear(out)
	if err := doc.TransactWithBudget(func(tx *Transaction) error { tx.GetMap("contents").Delete(tx, "key"); return nil }, &ProcessingBudget{ChangedKeys: out, ChangedKeysLimit: 256 << 10, ChangedKeysOverflow: &overflow}); err != nil {
		t.Fatal(err)
	}
	if !out["contents"]["key"] {
		t.Fatal("delete not tracked")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clear(out)
	if err := doc.TransactWithBudget(func(tx *Transaction) error { return nil }, &ProcessingBudget{Context: ctx, ChangedKeys: out, ChangedKeysLimit: 256 << 10, ChangedKeysOverflow: &overflow}); !errors.Is(err, context.Canceled) || len(out) != 0 {
		t.Fatal("cancellation", err, out)
	}
}

func TestChangedKeysBounded(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	out := map[string]map[string]bool{}
	overflow := false
	apply := func(key string) {
		t.Helper()
		if e := doc.TransactWithBudget(func(tx *Transaction) error { tx.GetMap("contents").Set(tx, key, "value"); return nil }, &ProcessingBudget{ChangedKeys: out, ChangedKeysLimit: 140, ChangedKeysOverflow: &overflow}); e != nil {
			t.Fatal(e)
		}
	}
	apply("a")
	apply("a")
	if overflow || len(out["contents"]) != 1 {
		t.Fatal("duplicate key consumed capacity")
	}
	apply("b")
	if !overflow || len(out) != 0 {
		t.Fatal("overflow must discard partial key set")
	}
	apply("c")
	if len(out) != 0 {
		t.Fatal("overflow must stay in full-capture mode")
	}
	overflow = false
	apply("d")
	if overflow || !out["contents"]["d"] {
		t.Fatal("reset did not resume tracking")
	}
}
