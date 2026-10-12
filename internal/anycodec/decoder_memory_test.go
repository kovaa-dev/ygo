package anycodec

import "testing"

func TestDecodedObjectsOwnValuesAndInputAfterKeyReuse(t *testing.T) {
	wire := []byte{118, 1, 1, 'k', 119, 1, 'v', 118, 1, 1, 'k', 119, 1, 'v'}
	decoder := NewDecoderWithBudget(wire, &DecodeBudget{CopyPayload: true})
	first, err := decoder.ReadAny(nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := decoder.ReadAny(nil)
	if err != nil {
		t.Fatal(err)
	}
	clear(wire)
	a, b := first.(map[string]any), second.(map[string]any)
	if a["k"] != "v" || b["k"] != "v" {
		t.Fatal("decoded strings alias input")
	}
	a["k"] = "changed"
	if b["k"] != "v" {
		t.Fatal("key reuse aliased mutable objects")
	}
	if len(decoder.keys) != 1 {
		t.Fatal("repeated field key was not reused")
	}
}
