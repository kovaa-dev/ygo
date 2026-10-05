package crdt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/reearth/ygo/encoding"
)

func TestUnit_BudgetRefusalIdentityAtEveryAllocation(t *testing.T) {
	source := New()
	defer source.Destroy()
	m := source.GetMap("m")
	source.Transact(func(tx *Transaction) {
		m.Set(tx, "nested", map[string]any{"array": []any{"text", []byte{1, 2, 3}, map[string]any{"flag": true}}})
		m.Set(tx, "second", "tail")
	})
	raw := EncodeStateAsUpdateV1(source, nil)
	sentinel := errors.New("test shared capacity exhausted")
	for _, name := range []string{"apply", "inspect", "encode", "local"} {
		t.Run(name, func(t *testing.T) {
			run := func(reserve func(uint64) error) error {
				doc := New()
				defer doc.Destroy()
				switch name {
				case "apply":
					return ApplyUpdateV1WithBudget(doc, raw, nil, &ProcessingBudget{Reserve: reserve})
				case "inspect":
					_, err := InspectUpdateV1(doc, raw, UpdateInspectionOptions{Reserve: reserve}, func(InspectedItem) error { return nil })
					return err
				case "encode":
					_, err := EncodeStateAsUpdateV1WithBudget(source, nil, &encoding.EncodeBudget{Reserve: reserve})
					return err
				default:
					target := doc.GetMap("m")
					return doc.TransactWithBudget(func(tx *Transaction) error {
						target.Set(tx, "nested", map[string]any{"array": []any{"text", []byte{1, 2, 3}}})
						return nil
					}, &ProcessingBudget{Reserve: reserve})
				}
			}
			total := 0
			if err := run(func(uint64) error { total++; return nil }); err != nil {
				t.Fatal(err)
			}
			if total < 3 {
				t.Fatalf("fixture has only %d allocations", total)
			}
			for fail := 1; fail <= total; fail++ {
				calls := 0
				err := run(func(uint64) error {
					calls++
					if calls == fail {
						return sentinel
					}
					return nil
				})
				if !errors.Is(err, sentinel) {
					t.Fatalf("allocation %d/%d lost identity: %v", fail, total, err)
				}
			}
		})
	}
}

func TestUnit_InspectionNestedCancellationIdentity(t *testing.T) {
	source := New()
	defer source.Destroy()
	m := source.GetMap("m")
	source.Transact(func(tx *Transaction) { m.Set(tx, "nested", map[string]any{"values": []any{"a", "b", "c"}}) })
	raw := EncodeStateAsUpdateV1(source, nil)
	target := New()
	defer target.Destroy()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	_, err := InspectUpdateV1(target, raw, UpdateInspectionOptions{Context: ctx, Reserve: func(uint64) error {
		calls++
		if calls == 5 {
			cancel()
		}
		return nil
	}}, func(InspectedItem) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation identity: %v calls=%d", err, calls)
	}
}

func TestUnit_NestedDecodeBudgetLimitIdentity(t *testing.T) {
	source := New()
	defer source.Destroy()
	m := source.GetMap("m")
	source.Transact(func(tx *Transaction) { m.Set(tx, "nested", map[string]any{"values": []any{"a", "b", "c"}}) })
	raw := EncodeStateAsUpdateV1(source, nil)
	for _, name := range []string{"apply", "inspect"} {
		for _, values := range []uint64{1, 4, 8} {
			t.Run(fmt.Sprintf("%s/%d", name, values), func(t *testing.T) {
				target := New()
				defer target.Destroy()
				var err error
				if name == "apply" {
					err = ApplyUpdateV1WithBudget(target, raw, nil, &ProcessingBudget{MaxValues: values})
				} else {
					_, err = InspectUpdateV1(target, raw, UpdateInspectionOptions{MaxValues: values}, nil)
				}
				if !errors.Is(err, encoding.ErrDecodeBudgetExceeded) {
					t.Fatalf("budget identity: %v", err)
				}
			})
		}
	}
}
