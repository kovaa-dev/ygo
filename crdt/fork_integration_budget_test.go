package crdt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// Both preflight and producer scheduling are new traversal/allocation paths.
// The server boundary must preserve refusal identity at every reservation.
func TestUnit_ForkIntegrationPendingBudgets(t *testing.T) {
	raw := pendingReverseChain(1, 64)
	sentinel := errors.New("shared server capacity exhausted")
	for _, cap := range []int{16, 100} {
		t.Run(fmt.Sprint(cap), func(t *testing.T) {
			run := func(b *ProcessingBudget) error {
				d := New(WithMaxPendingItems(cap))
				defer d.Destroy()
				err := ApplyUpdateV1WithBudget(d, raw, nil, b)
				if err == nil && (d.GetText("text").Len() != 64 || d.PendingStats().Items != 0) {
					t.Fatal("budgeted complete chain did not restore")
				}
				return err
			}
			total := 0
			if err := run(&ProcessingBudget{Reserve: func(uint64) error { total++; return nil }}); err != nil {
				t.Fatal(err)
			}
			for fail := 1; fail <= total; fail++ {
				calls := 0
				err := run(&ProcessingBudget{Reserve: func(uint64) error {
					calls++
					if calls == fail {
						return sentinel
					}
					return nil
				}})
				if !errors.Is(err, sentinel) {
					t.Fatalf("reservation %d/%d: %v", fail, total, err)
				}
			}
			for _, limit := range []uint64{1, 100, 1000} {
				if err := run(&ProcessingBudget{MaxWork: limit}); !errors.Is(err, ErrProcessingBudgetExceeded) {
					t.Fatalf("work limit %d: %v", limit, err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			err := run(&ProcessingBudget{Context: ctx, Reserve: func(uint64) error {
				calls++
				if calls == total/2 {
					cancel()
				}
				return nil
			}})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
		})
	}
}

func TestUnit_ForkIntegrationJSONBudget(t *testing.T) {
	// A 120-byte JSON string has a prefix that also looks like a lib0 Any tag.
	// Ambiguous wire probing must not construct values outside the budget.
	js := "[\"" + strings.Repeat("x", 116) + "\"]"
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1)
	enc.WriteVarString(js)
	dec := encoding.NewDecoderWithBudget(enc.Bytes(), &encoding.DecodeBudget{MaxValues: 8})
	doc := New()
	defer doc.Destroy()
	if _, err := decodeContent(dec, doc, wireJSON); !errors.Is(err, encoding.ErrDecodeBudgetExceeded) {
		t.Fatalf("JSON limit: %v", err)
	}
	called := false
	meter := encoding.NewEncoderWithBudget(&encoding.EncodeBudget{})
	encodeContent(meter, NewContentJSON(forkIntegrationMarshaler{called: &called}), 0)
	if called || !errors.Is(meter.Err(), ErrSemanticReadUnsupported) {
		t.Fatalf("unadmitted JSON marshaler ran: called=%v err=%v", called, meter.Err())
	}
}

type forkIntegrationMarshaler struct{ called *bool }

func (v forkIntegrationMarshaler) MarshalJSON() ([]byte, error) {
	*v.called = true
	return []byte(`"value"`), nil
}
