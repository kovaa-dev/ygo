package crdt

import (
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func resolverTestItems(doc *Doc, n int, shape string) []*Item {
	text := doc.GetText("text")
	root := doc.GetMap("root")
	items := make([]*Item, n)
	for i := range items {
		id := ClientID(i + 1)
		item := &Item{ID: ID{Client: id}, Parent: &text.abstractType, Content: NewContentString(fmt.Sprintf("%03d|", i+1))}
		switch shape {
		case "left":
			if i+1 < n {
				item.Origin = &ID{Client: id + 1}
			}
		case "right":
			if i+1 < n {
				item.OriginRight = &ID{Client: id + 1}
			}
		case "tree":
			if i > 0 {
				item.Origin = &ID{Client: ClientID((i-1)/2 + 1)}
			}
		case "missing":
			item.Origin = &ID{Client: 9999}
		case "cycle":
			item.Origin = &ID{Client: ClientID((i+1)%n + 1)}
		case "parents":
			item.Parent = nil
			item.Content = NewContentType(&NewMapPrelim().abstractType)
			key := "child"
			item.ParentSub = &key
			if i+1 < n {
				item.parentID = &ID{Client: id + 1}
			} else {
				item.Parent = &root.abstractType
			}
		}
		items[i] = item
	}
	return items
}

func TestUnit_PendingResolver_MatchesFixedPoint(t *testing.T) {
	const n = 128
	for _, shape := range []string{"left", "right", "tree", "missing", "cycle", "parents"} {
		for seed := int64(0); seed < 12; seed++ {
			t.Run(fmt.Sprintf("%s/%d", shape, seed), func(t *testing.T) {
				a, b := New(WithClientID(100001)), New(WithClientID(100001))
				defer a.Destroy()
				defer b.Destroy()
				var errs [2]error
				for i, doc := range []*Doc{a, b} {
					pending := resolverTestItems(doc, n, shape)
					rand.New(rand.NewSource(seed)).Shuffle(n, func(i, j int) { pending[i], pending[j] = pending[j], pending[i] })
					doc.Transact(func(txn *Transaction) {
						if i == 0 {
							errs[i] = referenceWithinUpdatePending(txn, pending)
						} else {
							errs[i] = resolveWithinUpdatePending(txn, pending)
						}
					})
				}
				require.NoError(t, errs[0])
				require.NoError(t, errs[1])
				require.Equal(t, a.StateVector(), b.StateVector())
				require.Equal(t, a.PendingStats(), b.PendingStats())
				require.Equal(t, a.GetText("text").ToString(), b.GetText("text").ToString())
				require.Equal(t, EncodeStateAsUpdateV1(a, nil), EncodeStateAsUpdateV1(b, nil))
				require.Equal(t, EncodeStateAsUpdateV2(a, nil), EncodeStateAsUpdateV2(b, nil))
			})
		}
	}
}

func TestUnit_PendingResolver_ImmutableRanges(t *testing.T) {
	items := []*Item{
		{ID: ID{Client: 1, Clock: 1}, Content: NewContentDeleted(100), Deleted: true},
		{ID: ID{Client: 1, Clock: 2}, Content: NewContentString("x")},
		{ID: ID{Client: 2}, Content: NewContentString("abc")},
	}
	order := indexPendingProducers(items)
	// Model GC integration trimming into the range. Search keys/ends stay frozen.
	items[0].ID.Clock = 50
	items[0].Content = items[0].Content.Splice(49)
	require.Equal(t, 0, findPendingProducer(order, ID{Client: 1, Clock: 1}))
	require.Equal(t, 2, findPendingProducer(order, ID{Client: 2, Clock: 2}))
	require.Equal(t, -1, findPendingProducer(order, ID{Client: 2, Clock: 3}))
	require.Equal(t, -1, findPendingProducer(order, ID{Client: 3}))
}

func TestUnit_PendingResolver_ReverseCheckpoint(t *testing.T) {
	const n = 128
	for _, version := range []int{1, 2} {
		source := New()
		text := source.GetText("text")
		groups := make(map[ClientID][]*Item, n)
		var want strings.Builder
		for i := 1; i <= n; i++ {
			item := &Item{ID: ID{Client: ClientID(i)}, Parent: &text.abstractType, Content: NewContentString(fmt.Sprintf("%03d|", i))}
			if version == 1 && i < n {
				item.Origin = &ID{Client: ClientID(i + 1), Clock: 3}
			}
			if version == 2 && i > 1 {
				item.Origin = &ID{Client: ClientID(i - 1), Clock: 3}
			}
			groups[item.ID.Client] = []*Item{item}
			value := i
			if version == 1 {
				value = n - i + 1
			}
			fmt.Fprintf(&want, "%03d|", value)
		}
		encode, apply := encodeStructStoreV1, ApplyUpdateV1
		if version == 2 {
			encode, apply = encodeStructStoreV2, ApplyUpdateV2
		}
		data := encode(groups, newDeleteSet(), nil, source.store)
		source.Destroy()
		target := New(WithMaxPendingItems(16))
		require.NoError(t, apply(target, data, nil))
		require.Zero(t, target.PendingStats().Items)
		require.Len(t, target.StateVector(), n)
		require.Equal(t, want.String(), target.GetText("text").ToString())
		target.Destroy()
	}
}

func TestUnit_PendingResolver_PersistentLimit(t *testing.T) {
	for _, resolve := range []func(*Transaction, []*Item) error{referenceWithinUpdatePending, resolveWithinUpdatePending} {
		doc := New(WithMaxPendingItems(16))
		text := doc.GetText("text")
		previous := []*Item{
			{ID: ID{Client: 900001}, Parent: &text.abstractType, Origin: &ID{Client: 9999}, Content: NewContentString("a")},
			{ID: ID{Client: 900002}, Parent: &text.abstractType, Origin: &ID{Client: 9999}, Content: NewContentString("b")},
		}
		var err error
		doc.Transact(func(txn *Transaction) { err = resolve(txn, previous) })
		require.NoError(t, err)
		pending := resolverTestItems(doc, 128, "missing")
		doc.Transact(func(txn *Transaction) { err = resolve(txn, pending) })
		require.ErrorIs(t, err, ErrInvalidUpdate)
		require.Equal(t, 16, doc.PendingStats().Items)
		require.Same(t, previous[0], doc.store.pending.items[0])
		require.Same(t, previous[1], doc.store.pending.items[1])
		require.Empty(t, doc.StateVector())
		doc.Destroy()
	}
}

// referenceWithinUpdatePending is main 07bd8f62's fixed-point resolver. It is
// independent of the scheduler, immutable index and shared retry-pass helper.
func referenceWithinUpdatePending(txn *Transaction, pending []*Item) error {
	for len(pending) > 0 {
		var remaining []*Item
		for _, item := range pending {
			if item.Origin != nil {
				if oi := txn.doc.store.Find(*item.Origin); oi != nil {
					item.Parent = oi.Parent
					if item.ParentSub == nil {
						item.ParentSub = oi.ParentSub
					}
				}
			}
			if item.Parent == nil && item.OriginRight != nil {
				if ori := txn.doc.store.Find(*item.OriginRight); ori != nil {
					item.Parent = ori.Parent
					if item.ParentSub == nil {
						item.ParentSub = ori.ParentSub
					}
				}
			}
			if item.Parent == nil && item.parentID != nil {
				if pi := txn.doc.store.Find(*item.parentID); pi != nil {
					if ct, ok := pi.Content.(*ContentType); ok {
						item.Parent = ct.Type
					}
				}
			}
			if item.Parent != nil {
				if _, _, isFuture := itemFutureDep(item, txn.doc.store); isFuture {
					remaining = append(remaining, item)
					continue
				}
				if item.Origin != nil {
					item.Left = txn.doc.store.getItemCleanEnd(txn, item.Origin.Client, item.Origin.Clock)
				}
				item.integrate(txn, 0)
			} else {
				remaining = append(remaining, item)
			}
		}
		if len(remaining) == len(pending) {
			for _, item := range remaining {
				if client, parkedAt, isFuture := itemFutureDep(item, txn.doc.store); isFuture {
					if txn.doc.store.pending != nil && len(txn.doc.store.pending.items) >= txn.doc.maxPendingItemsLimit() {
						return wrapUpdateErr(ErrInvalidUpdate)
					}
					if txn.doc.store.pending == nil {
						txn.doc.store.pending = &pendingUpdate{
							missing: make(StateVector),
						}
					}
					txn.doc.store.pending.items = append(txn.doc.store.pending.items, item)
					mergePendingMissing(txn.doc.store.pending.missing, client, parkedAt)
				} else {
					txn.doc.store.Append(item)
				}
			}
			break
		}
		pending = remaining
	}
	return nil
}

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

// Resource growth protects both decoder entry points: keeping V2's old inline
// retry loop would produce tens of MiB on this small reverse-chain checkpoint.
func TestUnit_PendingResolver_BoundsReverseChainBytes(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			data := pendingReverseChain(version, 2000)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			doc := New(WithMaxPendingItems(16))
			apply := ApplyUpdateV1
			if version == 2 {
				apply = ApplyUpdateV2
			}
			err := apply(doc, data, nil)
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Zero(t, doc.PendingStats().Items)
			require.Equal(t, 2000, doc.GetText("text").Len())
			require.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, uint64(16*1024*1024), "reverse-chain allocated bytes")
			doc.Destroy()
		})
	}
}
