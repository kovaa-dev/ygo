package crdt

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

func TestSemanticBudgetPreservesSharedValues(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	text := NewTextPrelim()
	array := NewArrayPrelim()
	nested := NewMapPrelim()
	doc.Transact(func(tx *Transaction) {
		m := tx.GetMap("values")
		m.Set(tx, "text", text)
		m.Set(tx, "array", array)
		m.Set(tx, "object", map[string]any{"yes": true})
		text.Insert(tx, 0, "hello 🚀", Attributes{"bold": true})
		text.InsertEmbed(tx, text.Len(), map[string]any{"nodeRef": map[string]any{"id": "node"}}, nil)
		array.Insert(tx, 0, []any{map[string]any{"value": "one"}})
		array.InsertType(tx, 1, nested)
		nested.Set(tx, "key", "nested")
	})
	var charged uint64
	budget := &encoding.EncodeBudget{Context: context.Background(), Reserve: func(n uint64) error { charged += n; return nil }}
	keys, err := doc.GetMap("values").KeysWithBudget(budget)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"array", "object", "text"}) {
		t.Fatalf("shared keys omitted: %v", keys)
	}
	delta, err := text.ToDeltaWithBudget(budget)
	if err != nil || !reflect.DeepEqual(delta, text.ToDelta()) {
		t.Fatalf("rich text changed: %v", err)
	}
	actual, err := array.ToJSONWithBudget(budget)
	expected, _ := array.ToJSON()
	if err != nil || !bytes.Equal(actual, expected) {
		t.Fatalf("array changed: %v %s != %s", err, actual, expected)
	}
	if charged == 0 {
		t.Fatal("no allocation admitted")
	}
}
func TestSemanticBudgetRejectsBeforeReadAndHonorsCancellation(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	text := NewTextPrelim()
	array := NewArrayPrelim()
	doc.Transact(func(tx *Transaction) {
		tx.GetMap("values").Set(tx, "text", text)
		tx.GetMap("values").Set(tx, "array", array)
		text.Insert(tx, 0, strings.Repeat("large", 1000), nil)
		array.Insert(tx, 0, []any{strings.Repeat("large", 1000)})
	})
	capacity := errors.New("capacity")
	budget := func() *encoding.EncodeBudget {
		return &encoding.EncodeBudget{Reserve: func(uint64) error { return capacity }}
	}
	if _, err := doc.GetMap("values").KeysWithBudget(budget()); !errors.Is(err, capacity) {
		t.Fatalf("keys: %v", err)
	}
	if _, err := text.ToDeltaWithBudget(budget()); !errors.Is(err, capacity) {
		t.Fatalf("text: %v", err)
	}
	if _, err := array.ToJSONWithBudget(budget()); !errors.Is(err, capacity) {
		t.Fatalf("array: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := text.ToDeltaWithBudget(&encoding.EncodeBudget{Context: ctx}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	if err := AdmitJSONValue(cycle, &encoding.EncodeBudget{MaxWork: 100}); !errors.Is(err, encoding.ErrEncodeBudgetExceeded) {
		t.Fatalf("cyclic traversal did not stop: %v", err)
	}
}
