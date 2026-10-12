package crdt

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/reearth/ygo/encoding"
)

// ErrProcessingBudgetExceeded identifies a cooperative processing refusal.
var ErrProcessingBudgetExceeded = errors.New("crdt: processing budget exceeded")

// ErrBudgetObservers identifies a document with subscriptions unsupported by the
// server-owned budgeted apply boundary. Use ordinary ApplyUpdateV1 for observers.
var ErrBudgetObservers = errors.New("crdt: budgeted mutation requires an observer-free document")

// ProcessingBudget is a single-use operational allocation/work budget. Reserve
// runs before charged allocations. Charges conservatively include Go containers
// and payload copies; they are not exact heap/RSS measurements. The caller owns
// reservation lifetime. AllocatedBytes is an upper charge usable until a resource
// recount, not proof that all charged bytes remain reachable.
type ProcessingBudget struct {
	// ChangedKeys collects changed keys of named root maps, including nested edits.
	// The caller owns the map and must discard it on transaction error. A positive
	// limit and overflow destination are required when collection is enabled.
	// Charges include key bytes and conservative bookkeeping, not an exact heap cap.
	ChangedKeys         map[string]map[string]bool
	ChangedKeysLimit    uint64
	ChangedKeysOverflow *bool
	// CompactObjects opts into read-only encoding.Object values for decoded Any objects.
	CompactObjects                                         bool
	Context                                                context.Context
	MaxWork, MaxAllocatedBytes, MaxValues, MaxPayloadBytes uint64
	Reserve                                                func(uint64) error
	work, allocated                                        uint64
	err                                                    error
}

// AllocatedBytes returns cumulative conservative allocation charges.
func (b *ProcessingBudget) AllocatedBytes() uint64 { return b.allocated }

// WorkUsed returns charged traversal steps, including repeated pending work.
func (b *ProcessingBudget) WorkUsed() uint64 { return b.work }
func (b *ProcessingBudget) step(n uint64) bool {
	if b == nil {
		return true
	}
	if b.err != nil {
		return false
	}
	if b.Context != nil {
		if err := b.Context.Err(); err != nil {
			b.err = err
			return false
		}
	}
	if n > ^uint64(0)-b.work || (b.MaxWork > 0 && (b.work > b.MaxWork || n > b.MaxWork-b.work)) {
		b.err = ErrProcessingBudgetExceeded
		return false
	}
	b.work += n
	return true
}
func (b *ProcessingBudget) allocate(n uint64) bool {
	if b == nil {
		return true
	}
	if !b.step(1) {
		return false
	}
	if n > ^uint64(0)-b.allocated || (b.MaxAllocatedBytes > 0 && (b.allocated > b.MaxAllocatedBytes || n > b.MaxAllocatedBytes-b.allocated)) {
		b.err = ErrProcessingBudgetExceeded
		return false
	}
	if b.Reserve != nil {
		if err := b.Reserve(n); err != nil {
			b.err = err
			return false
		}
	}
	b.allocated += n
	return true
}
func (t *Transaction) work(n uint64) bool     { return t.budget == nil || t.budget.step(n) }
func (t *Transaction) allocate(n uint64) bool { return t.budget == nil || t.budget.allocate(n) }
func (b *ProcessingBudget) decoder() *encoding.DecodeBudget {
	return &encoding.DecodeBudget{Context: b.Context, MaxValues: b.MaxValues, MaxPayloadBytes: b.MaxPayloadBytes, CopyPayload: true, CompactObjects: b.CompactObjects, Work: func(n uint64) error {
		if !b.step(n) {
			return b.err
		}
		return nil
	}, Reserve: func(n uint64) error {
		if !b.allocate(n) {
			return b.err
		}
		return nil
	}}
}

// ApplyUpdateV1WithBudget applies into an exclusively owned, observer-free server
// document. Any error may leave partial mutations: discard the document and
// restore durable state; never ACK, persist, broadcast or checkpoint it. Error
// paths skip commit cleanup and all observers. Successful calls retain ordinary
// integration, pending resolution, compaction and GC. Legacy APIs are unchanged.
func ApplyUpdateV1WithBudget(doc *Doc, update []byte, origin any, budget *ProcessingBudget) (err error) {
	return doc.TransactWithBudget(func(txn *Transaction) error { return applyV1Txn(txn, update) }, budget, origin)
}

// TransactWithBudget runs local mutations on an exclusively owned, observer-free
// document. Any error requires discarding the document, even if the callback
// returned the error. Supported local mutations are map Set/Delete, text
// Insert/InsertEmbed/Delete/Format, and array Insert/Push/Delete. Other local
// mutation APIs are not covered by this operational budget.
// Callback allocations outside CRDT methods must be charged
// by the caller. It must not call other lock-taking document methods.
func (doc *Doc) TransactWithBudget(fn func(*Transaction) error, budget *ProcessingBudget, origins ...any) (err error) {
	var origin any
	if len(origins) > 0 {
		origin = origins[0]
	}
	if budget == nil {
		budget = &ProcessingBudget{}
	}
	if !budget.step(0) {
		return budget.err
	}
	if budget.ChangedKeys != nil && (budget.ChangedKeysLimit == 0 || budget.ChangedKeysOverflow == nil) {
		return ErrInvalidUpdate
	}
	doc.mu.Lock()
	defer doc.mu.Unlock()
	doc.processingBudget = budget
	defer func() { doc.processingBudget = nil }()
	if len(doc.onUpdate)+len(doc.onAfterTxn)+len(doc.onSubdocs) > 0 || doc.undoManagerCount > 0 {
		return ErrBudgetObservers
	}
	for _, typ := range doc.share {
		if !budget.step(1) {
			return budget.err
		}
		if hasBudgetObservers(typ.baseType()) {
			return ErrBudgetObservers
		}
	}
	if !budget.allocate(uint64(len(doc.store.clients))*128 + 1024) {
		return budget.err
	}
	txn := &Transaction{doc: doc, Origin: origin, Local: true, deleteSet: newDeleteSet(), beforeState: doc.store.StateVector(), changed: make(map[*abstractType]map[string]struct{}, 4), ctx: budget.Context, budget: budget}
	if txn.ctx == nil {
		txn.ctx = context.Background()
	}
	defer func() {
		txn.done = true
		if v := recover(); v != nil {
			if _, ok := v.(budgetAbort); ok {
				err = budget.err
			} else {
				err = fmt.Errorf("%w: budgeted integration panic: %v", ErrInvalidUpdate, v)
			}
		}
		// Legacy decoder helpers may wrap nested failures as invalid-update text.
		// The budget owns the original refusal and must preserve its identity.
		if budget.err != nil {
			err = budget.err
		}
	}()
	if err = fn(txn); err != nil {
		return err
	}
	if !budget.step(0) {
		return budget.err
	}
	// Observer callbacks and their unbounded projection builders are outside this
	// server API. Newly discovered subscriptions require discarding this candidate.
	for typ := range txn.changed {
		for current := typ; current != nil; {
			if !budget.step(1) {
				return budget.err
			}
			if hasBudgetObservers(current) {
				return ErrBudgetObservers
			}
			if current.item == nil {
				break
			}
			current = current.item.Parent
		}
	}
	if budget.ChangedKeys != nil && !*budget.ChangedKeysOverflow {
		var changedBytes uint64
		for _, keys := range budget.ChangedKeys {
			for key := range keys {
				if !budget.step(1) {
					return budget.err
				}
				changedBytes += uint64(len(key) + 128)
			}
		}
		for typ, keys := range txn.changed {
			root := typ
			var nestedKey *string
			for root.item != nil {
				if !budget.step(1) {
					return budget.err
				}
				nestedKey = root.item.ParentSub
				root = root.item.Parent
				if root == nil {
					return ErrInvalidUpdate
				}
			}
			add := func(key string) bool {
				if !budget.step(1) {
					return false
				}
				if *budget.ChangedKeysOverflow {
					return true
				}
				if budget.ChangedKeys[root.name][key] {
					return true
				}
				cost := uint64(len(key) + 128)
				if changedBytes > budget.ChangedKeysLimit || cost > budget.ChangedKeysLimit-changedBytes {
					*budget.ChangedKeysOverflow = true
					clear(budget.ChangedKeys)
					return true
				}
				changedBytes += cost
				if !budget.allocate(uint64(len(key) + 128)) {
					return false
				}
				if budget.ChangedKeys[root.name] == nil {
					budget.ChangedKeys[root.name] = map[string]bool{}
				}
				budget.ChangedKeys[root.name][key] = true
				return true
			}
			if typ == root {
				for key := range keys {
					if !add(key) {
						return budget.err
					}
				}
			} else if nestedKey != nil {
				if !add(*nestedKey) {
					return budget.err
				}
			}
		}
	}
	if !budget.step(uint64(len(doc.store.clients))) || !budget.allocate(uint64(len(doc.store.clients))*128) {
		return budget.err
	}
	txn.afterState = doc.store.StateVector()
	squashRuns(txn)
	if !budget.step(0) {
		return budget.err
	}
	tryMergeWithLefts(txn)
	if !budget.step(0) {
		return budget.err
	}
	if doc.gc && doc.undoManagerCount == 0 {
		gcTxnDeleteSet(doc, txn)
	}
	if !budget.step(0) {
		return budget.err
	}
	for sd := range txn.subdocsAdded {
		if !budget.step(1) {
			return budget.err
		}
		doc.subdocs[sd.guid] = sd
	}
	for sd := range txn.subdocsRemoved {
		if !budget.step(1) {
			return budget.err
		}
		delete(doc.subdocs, sd.guid)
	}
	return nil
}
func hasBudgetObservers(t *abstractType) bool {
	if t.deep != nil && len(t.deep.entries) > 0 {
		return true
	}
	switch v := t.owner.(type) {
	case *YText:
		return len(v.observers) > 0
	case *YMap:
		return len(v.observers) > 0
	case *YArray:
		return len(v.observers) > 0
	case *YXmlFragment:
		return len(v.observers) > 0
	case *YXmlElement:
		return len(v.observers) > 0
	case *YXmlText:
		return len(v.observers) > 0
	}
	return false
}
func prepareBudgetSplit(txn *Transaction, item *Item) bool {
	switch c := item.Content.(type) {
	case *ContentString:
		return txn.allocate(uint64(len(c.Str))*3+32) && txn.work(uint64(len(c.Str)))
	case *ContentAny:
		return txn.allocate(uint64(len(c.Vals))*32 + 64)
	case *ContentJSON:
		return txn.allocate(uint64(len(c.Vals))*32 + 64)
	}
	return true
}
func ownSplitContent(content Content) {
	switch c := content.(type) {
	case *ContentString:
		c.Str = strings.Clone(c.Str)
	case *ContentAny:
		c.Vals = append([]any(nil), c.Vals...)
	case *ContentJSON:
		c.Vals = append([]any(nil), c.Vals...)
	}
}

func (txn *Transaction) mergePendingDeletes(ds DeleteSet) bool {
	for client, ranges := range ds.clients {
		n := uint64(len(ranges)) + uint64(len(txn.doc.store.pendingDs.clients[client]))
		if !txn.work(n*nLog(n)) || !txn.allocate(uint64(len(ranges))*32+128) {
			return false
		}
	}
	txn.doc.store.pendingDs.Merge(ds)
	return txn.work(0)
}
func nLog(n uint64) uint64 {
	var log uint64 = 1
	for n > 1 {
		log++
		n >>= 1
	}
	return log
}
