package crdt

import (
	"encoding/json"
	"fmt"
	"strings"
)

// budgetAbort is private control-flow unwinding for legacy void-return
// mutators. TransactWithBudget always converts it to the stored error before
// returning; it cannot escape that API. This narrowly scoped exception avoids
// allocations and partial cleanup after a refused operation.
type budgetAbort struct{}

// Check charges cooperative work and checks cancellation.
func (b *ProcessingBudget) Check(work uint64) error {
	if !b.step(work) {
		return b.err
	}
	return nil
}

// ReserveAllocation charges allocation before the caller creates a buffer.
func (b *ProcessingBudget) ReserveAllocation(bytes uint64) error {
	if !b.allocate(bytes) {
		return b.err
	}
	return nil
}

func (b *ProcessingBudget) mustWork(n uint64) {
	if !b.step(n) {
		panic(budgetAbort{})
	}
}
func (b *ProcessingBudget) mustAllocate(n uint64) {
	if !b.allocate(n) {
		panic(budgetAbort{})
	}
}
func (t *abstractType) localWork() {
	if t.doc != nil && t.doc.processingBudget != nil {
		t.doc.processingBudget.mustWork(1)
	}
}
func (txn *Transaction) localBudget() *ProcessingBudget {
	if txn == nil || txn.budget == nil {
		return nil
	}
	txn.budget.mustWork(1)
	return txn.budget
}

// Own plain JSON buffers before their values become document-owned. This also
// bounds recursive validation work before the legacy UTF-8 validators run.
func ownLocalValue(v any, b *ProcessingBudget) any {
	b.mustWork(1)
	switch x := v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v
	case string:
		b.mustWork(uint64(len(x)))
		b.mustAllocate(uint64(len(x)) + 32)
		return strings.Clone(x)
	case json.Number:
		b.mustWork(uint64(len(x)))
		b.mustAllocate(uint64(len(x)) + 32)
		return json.Number(strings.Clone(string(x)))
	case []byte:
		b.mustWork(uint64(len(x)))
		b.mustAllocate(uint64(len(x)) + 32)
		return append([]byte(nil), x...)
	case []any:
		b.mustAllocate(uint64(len(x))*32 + 1024)
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = ownLocalValue(v, b)
		}
		return out
	case map[string]any:
		b.mustAllocate(uint64(len(x))*256 + 1024)
		out := make(map[string]any, len(x))
		for k, v := range x {
			key := ownLocalValue(k, b).(string)
			out[key] = ownLocalValue(v, b)
		}
		return out
	case sharedType:
		b.mustAllocate(1024)
		return v
	default:
		b.err = fmt.Errorf("crdt: unsupported budgeted local value %T", v)
		panic(budgetAbort{})
	}
}
func ownLocalAttrs(attrs Attributes, b *ProcessingBudget) Attributes {
	if attrs == nil {
		return nil
	}
	return Attributes(ownLocalValue(map[string]any(attrs), b).(map[string]any))
}
