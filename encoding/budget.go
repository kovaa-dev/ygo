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
	Work            func(uint64) error
	Context         context.Context
	MaxValues       uint64
	MaxPayloadBytes uint64
	values          uint64
	payload         uint64
	Reserve         func(uint64) error
	CopyPayload     bool
	err             error
}

func (b *DecodeBudget) check() error {
	if b.err != nil {
		return b.err
	}
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
func (d *Decoder) ReserveValues(n uint64) (resultErr error) {
	if d.budget == nil {
		return nil
	}
	defer func() {
		if resultErr != nil && d.budget.err == nil {
			d.budget.err = resultErr
		}
	}()
	if d.budget.Work != nil {
		if err := d.budget.Work(n); err != nil {
			return err
		}
	}
	if err := d.budget.reserve(&d.budget.values, n, d.budget.MaxValues); err != nil {
		return err
	}
	if d.budget.Reserve != nil {
		if n > ^uint64(0)/128 {
			return ErrDecodeBudgetExceeded
		}
		return d.budget.Reserve(n * 128)
	}
	return nil
}

// ReservePayload reserves payload bytes before a caller materializes data that
// is not read through ReadVarBytes. ReadVarBytes already performs this charge.
func (d *Decoder) ReservePayload(n uint64) (resultErr error) {
	if d.budget == nil {
		return nil
	}
	defer func() {
		if resultErr != nil && d.budget.err == nil {
			d.budget.err = resultErr
		}
	}()
	if d.budget.Work != nil {
		if err := d.budget.Work(n); err != nil {
			return err
		}
	}
	if err := d.budget.reserve(&d.budget.payload, n, d.budget.MaxPayloadBytes); err != nil {
		return err
	}
	if d.budget.Reserve != nil {
		if n > ^uint64(0)/2 {
			return ErrDecodeBudgetExceeded
		}
		return d.budget.Reserve(n * 2)
	}
	return nil
}

// ReserveAllocation admits content-specific fixed/container allocations without
// consuming decoded value or payload counters. Legacy decoders ignore it.
func (d *Decoder) ReserveAllocation(n uint64) (resultErr error) {
	if d.budget == nil {
		return nil
	}
	defer func() {
		if resultErr != nil && d.budget.err == nil {
			d.budget.err = resultErr
		}
	}()
	if err := d.budget.check(); err != nil {
		return err
	}
	if d.budget.Reserve != nil {
		return d.budget.Reserve(n)
	}
	return nil
}

// BudgetError returns the first operational budget failure independently of
// content decoders that may wrap parser errors without preserving their cause.
func (d *Decoder) BudgetError() error {
	if d.budget == nil {
		return nil
	}
	return d.budget.err
}
