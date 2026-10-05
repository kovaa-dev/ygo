package crdt

import (
	"math/bits"
	"sort"

	"github.com/reearth/ygo/encoding"
)

// EncodeStateAsUpdateV1WithBudget uses the ordinary V1 content encoder with
// bounded output-buffer growth and cooperative traversal. On refusal it returns
// no bytes and leaves the document unchanged. Reservations are cumulative over
// output and known temporary buffers, not an exact heap/RSS measurement.
func EncodeStateAsUpdateV1WithBudget(doc *Doc, sv StateVector, budget *encoding.EncodeBudget) ([]byte, error) {
	doc.mu.Lock()
	defer doc.mu.Unlock()
	enc := encoding.NewEncoderWithBudget(budget)
	clients := budgetSortedClients(doc, enc)
	if enc.Err() != nil {
		return nil, enc.Err()
	}
	groups := 0
	for _, client := range clients {
		items := doc.store.clients[client]
		if len(items) > 0 && (items[len(items)-1].ID.Clock+uint64(items[len(items)-1].Content.Len())) > sv.Clock(client) {
			groups++
		}
	}
	enc.WriteVarUint(uint64(groups))
	for _, client := range clients {
		if !enc.Work(1) {
			break
		}
		items := doc.store.clients[client]
		clock := sv.Clock(client)
		index := sort.Search(len(items), func(i int) bool { return (items[i].ID.Clock + uint64(items[i].Content.Len())) > clock })
		if index == len(items) {
			continue
		}
		enc.WriteVarUint(uint64(len(items) - index))
		enc.WriteVarUint(uint64(client))
		enc.WriteVarUint(clock)
		for i, item := range items[index:] {
			if !enc.Work(1) {
				break
			}
			offset := 0
			if i == 0 && clock > item.ID.Clock {
				offset = int(clock - item.ID.Clock)
			}
			encodeItem(enc, item, offset, doc.store)
		}
	}
	// Count and stream compact deleted ranges without allocating a second map or
	// a slice per item. StructStore keeps each client's items in clock order.
	deletedClients := 0
	for _, client := range clients {
		if countDeletedRanges(doc.store.clients[client], enc, nil) > 0 {
			deletedClients++
		}
	}
	enc.WriteVarUint(uint64(deletedClients))
	for _, client := range clients {
		if !enc.Work(1) {
			break
		}
		items := doc.store.clients[client]
		count := countDeletedRanges(items, enc, nil)
		if count == 0 {
			continue
		}
		enc.WriteVarUint(uint64(client))
		enc.WriteVarUint(uint64(count))
		countDeletedRanges(items, enc, func(clock, length uint64) { enc.WriteVarUint(clock); enc.WriteVarUint(length) })
	}
	if err := enc.Err(); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// EncodeStateVectorV1WithBudget bounds state-vector allocation and encoding.
func EncodeStateVectorV1WithBudget(doc *Doc, budget *encoding.EncodeBudget) ([]byte, error) {
	doc.mu.Lock()
	defer doc.mu.Unlock()
	enc := encoding.NewEncoderWithBudget(budget)
	clients := budgetSortedClients(doc, enc)
	enc.WriteVarUint(uint64(len(clients)))
	for _, client := range clients {
		if !enc.Work(1) {
			break
		}
		enc.WriteVarUint(uint64(client))
		enc.WriteVarUint(doc.store.NextClock(client))
	}
	if err := enc.Err(); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func budgetSortedClients(doc *Doc, enc *encoding.Encoder) []ClientID {
	n := len(doc.store.clients)
	if !enc.Work(uint64(n)*uint64(max(1, bits.Len(uint(n))))) || !enc.ReserveAllocation(uint64(n)*8) {
		return nil
	}
	clients := make([]ClientID, 0, n)
	for client := range doc.store.clients {
		if !enc.Work(1) {
			return nil
		}
		clients = append(clients, client)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	enc.Work(0)
	return clients
}

func countDeletedRanges(items []*Item, enc *encoding.Encoder, emit func(uint64, uint64)) int {
	count := 0
	var clock, length uint64
	flush := func() {
		if length > 0 {
			count++
			if emit != nil {
				emit(clock, length)
			}
			length = 0
		}
	}
	for _, item := range items {
		if !enc.Work(1) {
			return count
		}
		if !item.Deleted {
			continue
		}
		if length > 0 && clock+length != item.ID.Clock {
			flush()
		}
		if length == 0 {
			clock = item.ID.Clock
		}
		length += uint64(item.Content.Len())
	}
	flush()
	return count
}
