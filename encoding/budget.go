package encoding

import "github.com/reearth/ygo/internal/anycodec"

// ErrDecodeBudgetExceeded identifies an operational decode budget refusal.
var ErrDecodeBudgetExceeded = anycodec.ErrDecodeBudgetExceeded

// DecodeBudget bounds decoder work/allocation inputs, not heap or RSS. Budgets
// are mutable, single-use and cannot be shared between concurrent operations.
type DecodeBudget = anycodec.DecodeBudget

// NewDecoderWithBudget keeps the normal parser with optional cumulative limits.
func NewDecoderWithBudget(data []byte, budget *DecodeBudget) *Decoder {
	return &Decoder{cursor: anycodec.NewDecoderWithBudget(data, budget)}
}

// ReserveValues admits value slots before collection allocation.
func (d *Decoder) ReserveValues(n uint64) error { return d.cursor.ReserveValues(n) }

// ReservePayload admits payload work before materialization.
func (d *Decoder) ReservePayload(n uint64) error { return d.cursor.ReservePayload(n) }

// ReserveAllocation admits content-specific fixed/container allocations.
func (d *Decoder) ReserveAllocation(n uint64) error { return d.cursor.ReserveAllocation(n) }

// BudgetError returns the original operational refusal independently of wrappers.
func (d *Decoder) BudgetError() error { return d.cursor.BudgetError() }
