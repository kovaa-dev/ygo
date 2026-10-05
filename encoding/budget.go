package encoding

import (
	"context"
	"errors"
)

// ErrDecodeBudgetExceeded identifies an operational decode budget refusal.
var ErrDecodeBudgetExceeded = errors.New("encoding: decode budget exceeded")

// DecodeBudget bounds cumulative decoded value slots and length-prefixed payload
// bytes. These are work/allocation inputs, not an exact Go heap or RSS limit.
// Zero limits are unlimited; legacy decoder limits still apply. A budget is
// mutable, single-use, and must not be shared between concurrent decoders.
type DecodeBudget struct {
	Context         context.Context
	MaxValues       uint64
	MaxPayloadBytes uint64
	values          uint64
	payload         uint64
}

func (b *DecodeBudget) check() error {
	if b.Context != nil {
		return b.Context.Err()
	}
	return nil
}

func (b *DecodeBudget) reserve(counter *uint64, n, limit uint64) error {
	if err := b.check(); err != nil {
		return err
	}
	if n > ^uint64(0)-*counter || (limit > 0 && (*counter > limit || n > limit-*counter)) {
		return ErrDecodeBudgetExceeded
	}
	*counter += n
	return nil
}

// NewDecoderWithBudget uses the normal lib0 parser with an optional cumulative
// budget. Refusal occurs before collection allocation or payload materialization.
func NewDecoderWithBudget(data []byte, budget *DecodeBudget) *Decoder {
	return &Decoder{buf: data, budget: budget}
}

// ReserveValues reserves value slots before callers allocate outer content
// containers. Nested values also consume slots, conservatively counting both
// container entries and the values decoded into them.
func (d *Decoder) ReserveValues(n uint64) error {
	if d.budget == nil {
		return nil
	}
	return d.budget.reserve(&d.budget.values, n, d.budget.MaxValues)
}

// ReservePayload reserves payload bytes before a caller materializes data that
// is not read through ReadVarBytes. ReadVarBytes already performs this charge.
func (d *Decoder) ReservePayload(n uint64) error {
	if d.budget == nil {
		return nil
	}
	return d.budget.reserve(&d.budget.payload, n, d.budget.MaxPayloadBytes)
}
