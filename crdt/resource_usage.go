package crdt

// ResourceUsage describes a conservative, version-specific resident charge for
// server-owned documents loaded through ApplyUpdateV1WithBudget. It counts known
// CRDT/container overhead and owned payloads, not process RSS or Go allocator
// metadata. Call at load/checkpoint or before rejecting accumulated delta charges;
// ordinary edits can retain their ProcessingBudget.AllocatedBytes until recount.
type ResourceUsage struct {
	Items, PendingItems, ValueSlots, PayloadBytes, RetainedBytes uint64
}

// ResourceUsage recounts integrated and pending state under a read lock. The
// supplied budget bounds traversal and accounting scratch work. It must not be
// called while holding a transaction or inspection callback on this document.
// Documents mutated by legacy APIs may retain aliased backing payloads: their
// payload charge cannot be used as a bound without reloading with budgeted apply.
func (d *Doc) ResourceUsage(budget *ProcessingBudget) (ResourceUsage, error) {
	if budget == nil {
		budget = &ProcessingBudget{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := ResourceUsage{RetainedBytes: 16384}
	add := func(n uint64) bool {
		if n > ^uint64(0)-result.RetainedBytes {
			budget.err = ErrProcessingBudgetExceeded
			return false
		}
		result.RetainedBytes += n
		return true
	}
	var value func(any) bool
	value = func(v any) bool {
		if !budget.step(1) {
			return false
		}
		result.ValueSlots++
		if !add(128) {
			return false
		}
		switch x := v.(type) {
		case string:
			result.PayloadBytes += uint64(len(x))
			return add(uint64(len(x)) * 2)
		case []byte:
			result.PayloadBytes += uint64(cap(x))
			return add(uint64(cap(x)))
		case []any:
			if !budget.allocate(512) {
				return false
			}
			if !add(uint64(cap(x))*16 + 128) {
				return false
			}
			for _, child := range x {
				if !value(child) {
					return false
				}
			}
		case map[string]any:
			if !budget.allocate(512) {
				return false
			}
			if !add(uint64(len(x))*128 + 256) {
				return false
			}
			for key, child := range x {
				result.PayloadBytes += uint64(len(key))
				if !add(uint64(len(key))) || !value(child) {
					return false
				}
			}
		}
		return true
	}
	itemUsage := func(item *Item) bool {
		if !budget.step(1) {
			return false
		}
		result.Items++
		if !add(512) {
			return false
		}
		if item.ParentSub != nil && !value(*item.ParentSub) {
			return false
		}
		switch c := item.Content.(type) {
		case *ContentString:
			return value(c.Str)
		case *ContentBinary:
			return value(c.Data)
		case *ContentAny:
			return value(c.Vals)
		case *ContentJSON:
			return value(c.Vals)
		case *ContentEmbed:
			return value(c.Val)
		case *ContentFormat:
			return value(c.Key) && value(c.Val)
		case *ContentType:
			if c.Type != nil {
				return value(c.Type.name)
			}
		case *ContentDoc:
			if c.Doc != nil {
				return add(16384 + uint64(len(c.Doc.guid)))
			}
		}
		return true
	}
	if !add(uint64(len(d.share))*512 + uint64(len(d.store.clients))*256) {
		return ResourceUsage{}, budget.err
	}
	for name := range d.share {
		if !budget.step(1) || !value(name) {
			return ResourceUsage{}, budget.err
		}
	}
	for _, items := range d.store.clients {
		if !add(uint64(cap(items)) * 8) {
			return ResourceUsage{}, budget.err
		}
		for _, item := range items {
			if !itemUsage(item) {
				return ResourceUsage{}, budget.err
			}
		}
	}
	if pending := d.store.pending; pending != nil {
		if !add(uint64(cap(pending.items))*8 + uint64(len(pending.missing))*128 + 256) {
			return ResourceUsage{}, budget.err
		}
		result.PendingItems = uint64(len(pending.items))
		for _, item := range pending.items {
			if !itemUsage(item) {
				return ResourceUsage{}, budget.err
			}
		}
	}
	for _, ranges := range d.store.pendingDs.clients {
		if !budget.step(uint64(len(ranges))) || !add(uint64(cap(ranges))*16+128) {
			return ResourceUsage{}, budget.err
		}
	}
	for _, items := range d.store.pendingMoves {
		if !budget.step(uint64(len(items))) || !add(uint64(cap(items))*8+128) {
			return ResourceUsage{}, budget.err
		}
	}
	if !budget.step(0) {
		return ResourceUsage{}, budget.err
	}
	return result, nil
}
