package crdt

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/reearth/ygo/encoding"
)

func TestCompactObjectInspectionEquality(t *testing.T) {
	original := map[string]any{"a": []any{int64(1), map[string]any{"b": "value"}}, "binary": []byte{1, 2}}
	e := encoding.NewEncoder()
	e.WriteAny(original)
	compact, err := encoding.NewDecoderWithBudget(e.Bytes(), &encoding.DecodeBudget{CompactObjects: true}).ReadAny()
	if err != nil {
		t.Fatal(err)
	}
	state := inspectionState{dec: encoding.NewDecoder(nil)}
	for _, pair := range [][2]any{{compact, original}, {original, compact}, {compact, compact}} {
		same, err := state.equalInspectionValue(pair[0], pair[1])
		if err != nil || !same {
			t.Fatal(same, err)
		}
	}
	original["binary"] = []byte{3}
	if same, err := state.equalInspectionValue(compact, original); err != nil || same {
		t.Fatal(same, err)
	}
	state.dec = encoding.NewDecoderWithBudget(nil, &encoding.DecodeBudget{MaxValues: 1})
	if _, err := state.equalInspectionValue(compact, compact); err == nil {
		t.Fatal("unbounded comparison")
	}
}

func TestCompactObjectsApplyReadAndRestore(t *testing.T) {
	source := New()
	defer source.Destroy()
	source.Transact(func(tx *Transaction) {
		tx.GetMap("root").Set(tx, "object", map[string]any{"a": []any{int64(1), map[string]any{"b": "text"}}})
		tx.GetArray("array").Push(tx, []any{map[string]any{"id": "one"}})
	})
	wire := EncodeStateAsUpdateV1(source, nil)
	target := New()
	defer target.Destroy()
	if err := ApplyUpdateV1WithBudget(target, wire, nil, &ProcessingBudget{CompactObjects: true}); err != nil {
		t.Fatal(err)
	}
	v, _ := target.GetMap("root").Get("object")
	if _, ok := v.(*encoding.Object); !ok {
		t.Fatalf("not compact: %T", v)
	}
	if err := AdmitJSONValue(v, nil); err != nil {
		t.Fatal(err)
	}
	want, _ := source.GetMap("root").Get("object")
	a, _ := json.Marshal(want)
	b, err := json.Marshal(v)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatal("semantic difference", err)
	}
	if !bytes.Equal(wire, EncodeStateAsUpdateV1(target, nil)) {
		t.Fatal("wire difference")
	}
	if _, err := target.ResourceUsage(&ProcessingBudget{}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.GetArray("array").ToJSONWithBudget(nil); err != nil {
		t.Fatal(err)
	}
	vector := source.StateVector()
	source.Transact(func(tx *Transaction) { tx.GetMap("root").Set(tx, "object", map[string]any{"replacement": true}) })
	if err := ApplyUpdateV1WithBudget(target, EncodeStateAsUpdateV1(source, vector), nil, &ProcessingBudget{CompactObjects: true}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(EncodeStateAsUpdateV1(source, nil), EncodeStateAsUpdateV1(target, nil)) {
		t.Fatal("edit difference")
	}
}
