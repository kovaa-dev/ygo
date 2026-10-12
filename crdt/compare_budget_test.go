package crdt

import (
	"bytes"
	"errors"
	"github.com/reearth/ygo/encoding"
	"testing"
)

func TestCompareStateBudgetPreservesRoundTrip(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	doc.Transact(func(tx *Transaction) {
		tx.GetText("text").Insert(tx, 0, "hello 🦊", nil)
		tx.GetMap("map").Set(tx, "object", map[string]any{"list": []any{int64(42), "v"}})
	})
	vector := doc.StateVector()
	doc.Transact(func(tx *Transaction) { tx.GetText("text").Delete(tx, 0, 1); tx.GetText("text").Insert(tx, 0, "H", nil) })
	for _, sv := range []StateVector{nil, vector, doc.StateVector()} {
		expected, err := EncodeStateAsUpdateV1WithBudget(doc, sv, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = CompareStateAsUpdateV1WithBudget(doc, sv, expected, &encoding.EncodeBudget{MaxBytes: uint64(len(expected)), MaxWork: 100000}); err != nil {
			t.Fatal(err)
		}
		changed := bytes.Clone(expected)
		changed[len(changed)-1] ^= 1
		if err = CompareStateAsUpdateV1WithBudget(doc, sv, changed, nil); !errors.Is(err, encoding.ErrEncodingMismatch) {
			t.Fatalf("accepted corrupted bytes: %v", err)
		}
		if err = CompareStateAsUpdateV1WithBudget(doc, sv, expected, &encoding.EncodeBudget{MaxBytes: 1}); !errors.Is(err, encoding.ErrEncodeBudgetExceeded) {
			t.Fatalf("ignored budget: %v", err)
		}
		unchanged, _ := EncodeStateAsUpdateV1WithBudget(doc, sv, nil)
		if !bytes.Equal(expected, unchanged) {
			t.Fatal("comparison mutated document")
		}
	}
}
