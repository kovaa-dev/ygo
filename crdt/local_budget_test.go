package crdt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestUnit_LocalBudgetHistoryShapeAndOwnership(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	m := doc.GetMap("contents")
	images := doc.GetMap("images")
	metadata := doc.GetMap("metadata")
	raw := []byte{1, 2, 3}
	object := map[string]any{"blob": raw, "nested": []any{"original"}}
	b := &ProcessingBudget{MaxWork: 1000000, MaxAllocatedBytes: 16 << 20}
	err := doc.TransactWithBudget(func(tx *Transaction) error {
		text := NewTextPrelim()
		m.Set(tx, "node", text)
		text.Insert(tx, 0, "hello", Attributes{"bold": true})
		text.InsertEmbed(tx, 5, map[string]any{"nodeRef": "other"}, nil)
		array := NewArrayPrelim()
		images.Set(tx, "node", array)
		array.Insert(tx, 0, []any{map[string]any{"id": "image"}})
		metadata.Set(tx, "node", object)
		return nil
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	wire := EncodeStateAsUpdateV1(doc, nil)
	clear(raw)
	object["nested"].([]any)[0] = "changed"
	if !bytes.Equal(wire, EncodeStateAsUpdateV1(doc, nil)) {
		t.Fatal("local input retained mutable caller storage")
	}
	target := New()
	defer target.Destroy()
	if err := ApplyUpdateV1(target, wire, nil); err != nil {
		t.Fatal(err)
	}
	if b.AllocatedBytes() == 0 {
		t.Fatal("missing charges")
	}
}
func TestUnit_LocalBudgetRefusesAndUnlocks(t *testing.T) {
	for _, kind := range []string{"map", "text", "array", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			doc := New()
			defer doc.Destroy()
			m := doc.GetMap("m")
			text := doc.GetText("text")
			a := doc.GetArray("a")
			b := &ProcessingBudget{MaxAllocatedBytes: 1500}
			if kind == "cancel" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				b.Context = ctx
			}
			err := doc.TransactWithBudget(func(tx *Transaction) error {
				switch kind {
				case "map":
					m.Set(tx, "key", map[string]any{"nested": []any{1, 2}})
				case "text":
					text.Insert(tx, 0, "value", nil)
				case "array":
					a.Insert(tx, 0, []any{1, 2})
				case "cancel":
					t.Fatal("called canceled callback")
				}
				return nil
			}, b)
			if !errors.Is(err, ErrProcessingBudgetExceeded) && !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			// A failed candidate must be discarded, but the API must release its mutex.
			doc.Destroy()
		})
	}
}
func TestUnit_LocalBudgetLargeMap(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	m := doc.GetMap("m")
	b := &ProcessingBudget{MaxWork: 10000000, MaxAllocatedBytes: 256 << 20}
	err := doc.TransactWithBudget(func(tx *Transaction) error {
		for i := 0; i < 100000; i++ {
			m.Set(tx, fmt.Sprint(i), "value")
		}
		return nil
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Keys()) != 100000 {
		t.Fatal(len(m.Keys()))
	}
}

func TestUnit_LocalBudgetFormattedTraversalAndObserverRefusal(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	text := doc.GetText("text")
	doc.Transact(func(tx *Transaction) {
		for i := 0; i < 1000; i++ {
			text.Insert(tx, i, "x", Attributes{"kind": i % 2})
		}
	})
	err := doc.TransactWithBudget(func(tx *Transaction) error { text.Insert(tx, 900, "z", Attributes{"bold": true}); return nil }, &ProcessingBudget{MaxWork: 30})
	if !errors.Is(err, ErrProcessingBudgetExceeded) {
		t.Fatalf("traversal error=%v", err)
	}
	observed := New()
	defer observed.Destroy()
	m := observed.GetMap("m")
	cancel := m.Observe(func(YMapEvent) { t.Fatal("observer called") })
	defer cancel()
	called := false
	err = observed.TransactWithBudget(func(tx *Transaction) error { called = true; m.Set(tx, "key", 1); return nil }, &ProcessingBudget{})
	if !errors.Is(err, ErrBudgetObservers) || called {
		t.Fatalf("observer refusal=%v callback=%v", err, called)
	}
}
