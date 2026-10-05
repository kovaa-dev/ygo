package crdt

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/reearth/ygo/encoding"
)

func inspectionFixture() (*Doc, []byte, []byte) {
	src := New(WithClientID(1), WithGC(false))
	root := src.GetMap("contents")
	txt := NewTextPrelim()
	src.Transact(func(txn *Transaction) { root.Set(txn, "node", txt) })
	first := EncodeStateAsUpdateV1(src, nil)
	sv := src.StateVector()
	src.Transact(func(txn *Transaction) { txt.Insert(txn, 0, "hello", nil) })
	return src, first, EncodeStateAsUpdateV1(src, sv)
}
func TestUnit_InspectUpdateV1_AncestryAndAtomicRejection(t *testing.T) {
	src, _, _ := inspectionFixture()
	dst := New(WithClientID(2))
	before := EncodeStateAsUpdateV1(dst, nil)
	var nested bool
	_, err := InspectUpdateV1(dst, EncodeStateAsUpdateV1(src, nil), UpdateInspectionOptions{}, func(v InspectedItem) error {
		if v.Unresolved || v.Root != "contents" {
			t.Fatalf("unexpected view: %+v", v)
		}
		if v.ContentKind == "string" {
			nested = true
			if len(v.Parents) != 1 || v.Parents[0].Kind != "text" || v.Parents[0].Key == nil || *v.Parents[0].Key != "node" {
				t.Fatalf("ancestry: %+v", v)
			}
		}
		return nil
	})
	if err != nil || !nested {
		t.Fatalf("inspect: %v, nested=%v", err, nested)
	}
	rejected := errors.New("schema rejected")
	_, err = InspectUpdateV1(dst, EncodeStateAsUpdateV1(src, nil), UpdateInspectionOptions{}, func(InspectedItem) error { return rejected })
	if !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	if !bytes.Equal(before, EncodeStateAsUpdateV1(dst, nil)) || len(dst.share) != 0 {
		t.Fatal("inspection mutated target")
	}
}
func TestUnit_InspectUpdateV1_PendingDoesNotChangeACKSemantics(t *testing.T) {
	_, first, later := inspectionFixture()
	dst := New(WithClientID(2))
	result, err := InspectUpdateV1(dst, later, UpdateInspectionOptions{}, nil)
	if err != nil || result.Unresolved != 1 {
		t.Fatalf("missing parent: %+v %v", result, err)
	}
	if err := ApplyUpdateV1(dst, later, nil); err != nil {
		t.Fatal(err)
	}
	pending := dst.PendingStats().Items
	var sawPending bool
	result, err = InspectUpdateV1(dst, first, UpdateInspectionOptions{}, func(v InspectedItem) error {
		if v.Pending {
			sawPending = true
			if v.Root != "contents" {
				t.Fatalf("pending root: %+v", v)
			}
		}
		return nil
	})
	if err != nil || result.Unresolved != 0 || !sawPending {
		t.Fatalf("resolution: %+v %v", result, err)
	}
	if dst.PendingStats().Items != pending {
		t.Fatal("inspection consumed pending")
	}
	if err := ApplyUpdateV1(dst, first, nil); err != nil {
		t.Fatal(err)
	}
	if dst.PendingStats().Items != 0 {
		t.Fatal("legal out-of-order update stopped resolving")
	}
}
func TestUnit_InspectUpdateV1_BudgetsAndMalformedLeaveTargetUntouched(t *testing.T) {
	src, _, _ := inspectionFixture()
	update := EncodeStateAsUpdateV1(src, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		opts UpdateInspectionOptions
		data []byte
		want error
	}{
		{"structs", UpdateInspectionOptions{MaxStructs: 1}, update, encoding.ErrDecodeBudgetExceeded},
		{"values", UpdateInspectionOptions{MaxValues: 1}, update, encoding.ErrDecodeBudgetExceeded},
		{"payload", UpdateInspectionOptions{MaxPayloadBytes: 1}, update, encoding.ErrDecodeBudgetExceeded},
		{"canceled", UpdateInspectionOptions{Context: ctx}, update, context.Canceled},
		{"truncated", UpdateInspectionOptions{}, update[:len(update)-1], ErrInvalidUpdate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := New()
			before := EncodeStateAsUpdateV1(dst, nil)
			_, err := InspectUpdateV1(dst, tc.data, tc.opts, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("%v, want %v", err, tc.want)
			}
			if !bytes.Equal(before, EncodeStateAsUpdateV1(dst, nil)) || len(dst.share) != 0 {
				t.Fatal("target changed")
			}
		})
	}
}
func BenchmarkInspectUpdateV1_Incremental(b *testing.B) {
	src, first, later := inspectionFixture()
	_ = src
	dst := New()
	if err := ApplyUpdateV1(dst, first, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := InspectUpdateV1(dst, later, UpdateInspectionOptions{}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func TestUnit_InspectUpdateV1_EmptyRootIsResolved(t *testing.T) {
	// Named empty-root map entry, encoded directly because local staging treats
	// an empty name as detached. The wire root name is nevertheless valid.
	update := []byte{1, 1, 1, 0, 40, 1, 0, 1, 107, 1, 120, 0}
	dst := New()
	seen := false
	result, err := InspectUpdateV1(dst, update, UpdateInspectionOptions{}, func(v InspectedItem) error {
		seen = true
		if v.Unresolved || v.Orphan || v.Root != "" || v.Key == nil {
			t.Fatalf("empty root: %+v", v)
		}
		return nil
	})
	if err != nil || !seen || result.Unresolved != 0 {
		t.Fatalf("result %+v %v", result, err)
	}
}

func TestUnit_InspectUpdateV1_ExactPendingDuplicateOnly(t *testing.T) {
	_, _, later := inspectionFixture()
	dst := New(WithClientID(2))
	if err := ApplyUpdateV1(dst, later, nil); err != nil {
		t.Fatal(err)
	}
	check := func(data []byte, want bool) {
		t.Helper()
		seen := false
		_, err := InspectUpdateV1(dst, data, UpdateInspectionOptions{}, func(v InspectedItem) error {
			if !v.Pending {
				seen = true
				if v.Existing != want {
					t.Fatalf("Existing=%v, want %v", v.Existing, want)
				}
			}
			return nil
		})
		if err != nil || !seen {
			t.Fatalf("inspect %v seen=%v", err, seen)
		}
	}
	check(later, true)
	forged := bytes.Replace(later, []byte("hello"), []byte("jello"), 1)
	check(forged, false)
	if dst.PendingStats().Items != 1 {
		t.Fatal("retained pending changed")
	}
}
func TestUnit_InspectUpdateV1_FormatAttribute(t *testing.T) {
	src := New(WithClientID(1))
	text := src.GetText("body")
	src.Transact(func(txn *Transaction) { text.Insert(txn, 0, "x", map[string]any{"bold": true}) })
	seen := false
	_, err := InspectUpdateV1(New(), EncodeStateAsUpdateV1(src, nil), UpdateInspectionOptions{}, func(v InspectedItem) error {
		if v.ContentKind == "format" {
			seen = true
			if v.Attribute != "bold" {
				t.Fatalf("attribute=%q", v.Attribute)
			}
		}
		return nil
	})
	if err != nil || !seen {
		t.Fatalf("format %v seen=%v", err, seen)
	}
}
