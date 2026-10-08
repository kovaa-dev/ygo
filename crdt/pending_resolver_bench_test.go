//go:build benchheavy

package crdt

import (
	"errors"
	"fmt"
	"testing"
)

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

func pendingLinearClientQueue(version, n int, complete bool) []byte {
	source := New()
	defer source.Destroy()
	root := source.GetMap("root")
	key := "child"
	parentClient := ClientID(n + 100)
	if version == 2 {
		parentClient = 1
	}
	parent := &Item{ID: ID{Client: parentClient}, Parent: &root.abstractType, ParentSub: &key, Content: NewContentType(&NewMapPrelim().abstractType)}
	groups := make(map[ClientID][]*Item, n+1)
	for i := 0; i < n; i++ {
		name := fmt.Sprint(i)
		id := ClientID(i + 100)
		groups[id] = []*Item{{ID: ID{Client: id}, parentID: &parent.ID, ParentSub: &name, Content: NewContentAny(i)}}
	}
	if complete {
		groups[parentClient] = []*Item{parent}
	}
	encode := encodeStructStoreV1
	if version == 2 {
		encode = encodeStructStoreV2
	}
	return encode(groups, newDeleteSet(), nil, source.store)
}

func BenchmarkPendingLinearClientQueue(b *testing.B) {
	const n = 20000
	for _, version := range []int{1, 2} {
		for _, complete := range []bool{true, false} {
			data := pendingLinearClientQueue(version, n, complete)
			b.Run(fmt.Sprintf("V%d/complete=%t", version, complete), func(b *testing.B) {
				apply := ApplyUpdateV1
				if version == 2 {
					apply = ApplyUpdateV2
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					doc := New(WithMaxPendingItems(n + 1))
					b.StartTimer()
					err := apply(doc, data, nil)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					if !complete {
						if doc.PendingStats().Items != n {
							b.Fatal("missing queue changed")
						}
					} else {
						val, ok := doc.GetMap("root").Get("child")
						if !ok || len(val.(*YMap).Entries()) != n || doc.PendingStats().Items != 0 {
							b.Fatal("complete queue changed")
						}
					}
					doc.Destroy()
					b.StartTimer()
				}
			})
		}
	}
}
