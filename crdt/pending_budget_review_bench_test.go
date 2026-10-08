package crdt

import (
	"errors"
	"fmt"
	"testing"
)

// pendingReverseChain puts each origin in the next wire client group. Each
// client has one item; only the last group can integrate on the first pass.
func pendingReverseChain(version, n int) []byte {
	doc := New()
	defer doc.Destroy()
	text := doc.GetText("text")
	groups := make(map[ClientID][]*Item, n)
	for i := 1; i <= n; i++ {
		client := ClientID(i)
		var origin *ID
		if version == 1 && i < n {
			origin = &ID{Client: client + 1}
		}
		if version == 2 && i > 1 {
			origin = &ID{Client: client - 1}
		}
		groups[client] = []*Item{{ID: ID{Client: client}, Parent: &text.abstractType, Origin: origin, Content: NewContentString("x")}}
	}
	if version == 2 {
		return encodeStructStoreV2(groups, newDeleteSet(), nil, doc.store)
	}
	return encodeStructStoreV1(groups, newDeleteSet(), nil, doc.store)
}

// pendingManyClientCheckpoint uses ordinary text insertions and Ygo's state
// encoder. Assigning each transaction a distinct client ID creates the fixture
// without constructing thousands of replicas during benchmark setup.
func pendingManyClientCheckpoint(n int) []byte {
	doc := New()
	defer doc.Destroy()
	text := doc.GetText("text")
	for i := 0; i < n; i++ {
		doc.Transact(func(txn *Transaction) {
			doc.clientID = ClientID(i + 1)
			text.Insert(txn, i, "x", nil)
		})
	}
	return EncodeStateAsUpdateV2(doc, nil)
}

// Report rejections separately: main's early rejection at a small pending cap
// is not a faster successful restore. All versions receive identical fixtures
// and options. The high-cap cases also compare successful apply on main.
func benchmarkPendingComplete(b *testing.B, update []byte, version, n, cap int) {
	apply := ApplyUpdateV1
	if version == 2 {
		apply = ApplyUpdateV2
	}
	b.ReportAllocs()
	rejected := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		doc := New(WithMaxPendingItems(cap))
		b.StartTimer()
		err := apply(doc, update, nil)
		b.StopTimer()
		if err != nil {
			if !errors.Is(err, ErrInvalidUpdate) {
				b.Fatal(err)
			}
			rejected++
		} else if doc.GetText("text").Len() != n || len(doc.StateVector()) != n || doc.PendingStats().Items != 0 {
			b.Fatal("complete update did not restore all clients and text")
		}
		doc.Destroy()
		b.StartTimer()
	}
	b.ReportMetric(float64(rejected)/float64(b.N), "rejections/op")
}

func BenchmarkPendingReverseChain(b *testing.B) {
	for _, version := range []int{1, 2} {
		for _, n := range []int{1000, 20000} {
			update := pendingReverseChain(version, n)
			for _, cap := range []int{16, n + 1} {
				b.Run(fmt.Sprintf("V%d/n=%d/cap=%d", version, n, cap), func(b *testing.B) { benchmarkPendingComplete(b, update, version, n, cap) })
			}
		}
	}
}

func BenchmarkPendingManyClientCheckpoint(b *testing.B) {
	const n = 10000
	update := pendingManyClientCheckpoint(n)
	for _, cap := range []int{16, n + 1} {
		b.Run(fmt.Sprintf("V2/n=%d/cap=%d", n, cap), func(b *testing.B) { benchmarkPendingComplete(b, update, 2, n, cap) })
	}
}
