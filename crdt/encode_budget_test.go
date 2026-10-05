package crdt

import (
	"bytes"
	"errors"
	"testing"

	"github.com/reearth/ygo/encoding"
)

func TestUnit_EncodeStateBudgetMatchesLegacyAndRefuses(t *testing.T) {
	d := New()
	defer d.Destroy()
	m := d.GetMap("map")
	txt := d.GetText("text")
	d.Transact(func(tx *Transaction) {
		m.Set(tx, "a", map[string]any{"b": []any{1, "two"}})
		txt.Insert(tx, 0, "hello world", nil)
	})
	sv, err := DecodeStateVectorV1(EncodeStateVectorV1(d))
	if err != nil {
		t.Fatal(err)
	}
	d.Transact(func(tx *Transaction) { txt.Delete(tx, 1, 2); m.Set(tx, "a", false) })
	for _, vector := range []StateVector{nil, sv} {
		want := EncodeStateAsUpdateV1(d, vector)
		got, err := EncodeStateAsUpdateV1WithBudget(d, vector, &encoding.EncodeBudget{MaxBytes: uint64(len(want)), MaxWork: 100000})
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("got=%x want=%x err=%v", got, want, err)
		}
		if got, err = EncodeStateAsUpdateV1WithBudget(d, vector, &encoding.EncodeBudget{MaxBytes: 1}); !errors.Is(err, encoding.ErrEncodeBudgetExceeded) || got != nil {
			t.Fatalf("partial output=%v err=%v", got, err)
		}
	}
	got, err := EncodeStateVectorV1WithBudget(d, &encoding.EncodeBudget{MaxBytes: 100, MaxWork: 100})
	if err != nil || !bytes.Equal(got, EncodeStateVectorV1(d)) {
		t.Fatal(err)
	}
}
