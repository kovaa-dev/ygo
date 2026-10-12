package crdt

import "testing"

func TestMergeCleanupChargesOneScanPerClient(t *testing.T) {
	doc := New(WithGC(false))
	defer doc.Destroy()
	const count = 20000
	items := make([]*Item, count)
	for i := range items {
		items[i] = &Item{ID: ID{Client: 1, Clock: uint64(i)}, Content: NewContentDeleted(1), Deleted: true}
		if i > 0 {
			items[i].Left = items[i-1]
			items[i-1].Right = items[i]
			items[i].Origin = &ID{Client: 1, Clock: uint64(i - 1)}
		}
	}
	doc.store.clients[1] = items
	budget := &ProcessingBudget{MaxWork: 100000}
	txn := &Transaction{doc: doc, budget: budget, mergeStructs: append([]*Item(nil), items[1:]...)}
	tryMergeWithLefts(txn)
	if budget.err != nil {
		t.Fatal(budget.err)
	}
	kept := doc.store.clients[1]
	if len(kept) != 1 || kept[0].Content.Len() != count || kept[0].Right != nil {
		t.Fatal("merge changed the retained clock range or links", len(kept))
	}
	for _, item := range items[1:] {
		if item != nil {
			t.Fatal("removed backing slots retain items")
		}
	}
}

func TestSharedTypesAllocateKeyStorageOnDemand(t *testing.T) {
	doc := New()
	defer doc.Destroy()
	text := doc.GetText("text")
	entries := doc.GetMap("entries")
	if text.itemMap != nil || entries.itemMap != nil {
		t.Fatal("empty type eagerly owns a key map")
	}
	doc.Transact(func(tx *Transaction) {
		text.Insert(tx, 0, "value", nil)
		entries.Set(tx, "", "empty key")
	}, nil)
	if text.itemMap != nil {
		t.Fatal("sequence content allocated key storage")
	}
	if value, ok := entries.Get(""); !ok || value != "empty key" {
		t.Fatal("lazy map lost empty-string key")
	}
	reloaded := New()
	defer reloaded.Destroy()
	if err := ApplyUpdateV1(reloaded, EncodeStateAsUpdateV1(doc, nil), nil); err != nil {
		t.Fatal(err)
	}
	if value, ok := reloaded.GetMap("entries").Get(""); !ok || value != "empty key" {
		t.Fatal("reloaded lazy map lost key")
	}
	if reloaded.GetText("text").itemMap != nil {
		t.Fatal("decoded sequence allocated key storage")
	}
}
