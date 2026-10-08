package crdt

import (
	"slices"
	"sort"
)

// Small queues cost less to retry directly than to index.
const pendingScheduleThreshold = 32

// retryWithinUpdatePending preserves wire order and reuses the queue's storage.
func retryWithinUpdatePending(txn *Transaction, pending []*Item) []*Item {
	remaining := pending[:0]
	for _, item := range pending {
		txn.budget.mustWork(1)
		if !tryIntegrate(txn, item) {
			remaining = append(remaining, item)
		}
	}
	return remaining
}

// pendingProducer freezes a wire range even if integration trims its Item.
type pendingProducer struct {
	id    ID
	end   uint64
	index int
}

// indexPendingProducers builds one sorted array without per-client maps.
func indexPendingProducers(pending []*Item) []pendingProducer {
	order := make([]pendingProducer, len(pending))
	for i, item := range pending {
		order[i] = pendingProducer{item.ID, item.ID.Clock + uint64(item.Content.Len()), i}
	}
	slices.SortFunc(order, func(a, b pendingProducer) int {
		if a.id.Client < b.id.Client {
			return -1
		}
		if a.id.Client > b.id.Client {
			return 1
		}
		if a.id.Clock < b.id.Clock {
			return -1
		}
		if a.id.Clock > b.id.Clock {
			return 1
		}
		return 0
	})
	return order
}

// findPendingProducer returns a range candidate; overlaps fall back to retries.
func findPendingProducer(order []pendingProducer, id ID) int {
	i := sort.Search(len(order), func(i int) bool {
		return order[i].id.Client > id.Client || order[i].id.Client == id.Client && order[i].id.Clock > id.Clock
	}) - 1
	if i >= 0 && order[i].id.Client == id.Client && id.Clock < order[i].end {
		return order[i].index
	}
	return -1
}

// resolvePendingDependencies walks producers before their dependents. Immutable
// ranges keep searches valid when GC integration trims an Item. Unresolvable or
// overlapping ranges remain for the ordinary fixed-point resolver.
func resolvePendingDependencies(txn *Transaction, pending []*Item) []*Item {
	n := uint64(len(pending))
	txn.budget.mustWork(n * nLog(n))
	txn.budget.mustAllocate(n*64 + 256)
	order := indexPendingProducers(pending)
	txn.budget.mustWork(0)
	state := make([]byte, len(pending))
	stack := make([]int, 0, len(pending))
	for start := range pending {
		txn.budget.mustWork(1)
		if state[start] != 0 {
			continue
		}
		stack = append(stack, start)
		for len(stack) > 0 {
			txn.budget.mustWork(1)
			index := stack[len(stack)-1]
			item := pending[index]
			state[index] = 1
			if tryIntegrate(txn, item) {
				state[index] = 2
				stack = stack[:len(stack)-1]
				continue
			}
			var deps [4]ID
			n := 0
			if end := txn.doc.store.NextClock(item.ID.Client); item.ID.Clock > end {
				deps[n] = ID{Client: item.ID.Client, Clock: end}
				n++
			}
			for _, dep := range []*ID{item.Origin, item.OriginRight, item.parentID} {
				if dep != nil && dep.Clock >= txn.doc.store.NextClock(dep.Client) {
					deps[n] = *dep
					n++
				}
			}
			pushed := false
			for _, dep := range deps[:n] {
				txn.budget.mustWork(nLog(uint64(len(order))))
				if next := findPendingProducer(order, dep); next >= 0 && state[next] == 0 {
					stack = append(stack, next)
					pushed = true
					break
				}
			}
			if !pushed {
				state[index] = 3
				stack = stack[:len(stack)-1]
			}
		}
	}
	remaining := pending[:0]
	for i, item := range pending {
		if state[i] != 2 {
			remaining = append(remaining, item)
		}
	}
	return remaining
}

// parkWithinUpdatePending charges only unresolved items to the persistent cap.
func parkWithinUpdatePending(txn *Transaction, pending []*Item) error {
	for _, item := range pending {
		if !txn.work(1) {
			return txn.budget.err
		}
		if txn.doc.store.pending != nil && len(txn.doc.store.pending.items) >= txn.doc.maxPendingItemsLimit() {
			return wrapUpdateErr(ErrInvalidUpdate)
		}
		if txn.doc.store.pending == nil {
			txn.doc.store.pending = &pendingUpdate{missing: make(StateVector)}
		}
		txn.doc.store.pending.items = append(txn.doc.store.pending.items, item)
		if client, parkedAt, future := itemFutureDep(item, txn.doc.store); future {
			mergePendingMissing(txn.doc.store.pending.missing, client, parkedAt)
		} else {
			mergePendingMissing(txn.doc.store.pending.missing, item.ID.Client, txn.doc.store.NextClock(item.ID.Client))
		}
	}
	return nil
}
