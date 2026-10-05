package encoding

import (
	"errors"
	"testing"
)

func TestUnit_DecoderBudgetErrorRetainsFirstFailure(t *testing.T) {
	sentinel := errors.New("capacity")
	d := NewDecoderWithBudget(nil, &DecodeBudget{Reserve: func(uint64) error { return sentinel }})
	if err := d.ReserveAllocation(1); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if !errors.Is(d.BudgetError(), sentinel) {
		t.Fatal(d.BudgetError())
	}
	if NewDecoder(nil).BudgetError() != nil {
		t.Fatal("legacy decoder budget error")
	}
}
