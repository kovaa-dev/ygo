package crdt

import (
	"bytes"
	"sort"
)

// existingPending only grandfathers an exact retransmission. Clock coverage alone
// is insufficient: decode can create roots even for subsequently skipped items.
func (s *inspectionState) existingPending(item *Item, view InspectedItem) (bool, error) {
	items := s.pending[item.ID.Client]
	start := sort.Search(len(items), func(i int) bool { return items[i].ID.Clock >= item.ID.Clock })
	for _, old := range items[start:] {
		if err := s.dec.ReserveValues(1); err != nil {
			return false, err
		}
		if old.ID.Clock > item.ID.Clock {
			break
		}
		if old.ID != item.ID || old.Content.Len() != item.Content.Len() {
			continue
		}
		if !sameInspectionID(old.Origin, item.Origin) || !sameInspectionID(old.OriginRight, item.OriginRight) || !sameInspectionID(inspectionParentID(old), inspectionParentID(item)) {
			continue
		}
		previous, err := s.view(old)
		if err != nil {
			return false, err
		}
		if previous.Root != view.Root || previous.Unresolved != view.Unresolved || previous.Orphan != view.Orphan || !sameInspectionKey(previous.Key, view.Key) || previous.ContentKind != view.ContentKind || previous.TypeKind != view.TypeKind || previous.Attribute != view.Attribute || len(previous.Parents) != len(view.Parents) {
			continue
		}
		parentsEqual := true
		for i, p := range previous.Parents {
			if p.Kind != view.Parents[i].Kind || !sameInspectionKey(p.Key, view.Parents[i].Key) {
				parentsEqual = false
				break
			}
		}
		if !parentsEqual {
			continue
		}
		switch item.Content.(type) {
		case *ContentDeleted:
			return true, nil
		case *ContentType:
			// Only metadata-free types used by the application. XML/doc/move content
			// needs additional comparison and is deliberately not grandfathered.
			return view.TypeKind == "text" || view.TypeKind == "array" || view.TypeKind == "map", nil
		case *ContentString:
			return s.equalInspectionValue(previous.Text, view.Text)
		case *ContentAny, *ContentJSON, *ContentEmbed, *ContentFormat:
			equal, err := s.equalInspectionValue(previous.Values, view.Values)
			if err != nil {
				return false, err
			}
			if equal {
				return true, nil
			}
		}
	}
	return false, nil
}

func inspectionParentID(item *Item) *ID {
	if item.parentID != nil {
		return item.parentID
	}
	if item.Parent != nil && item.Parent.item != nil {
		return &item.Parent.item.ID
	}
	return nil
}
func sameInspectionID(a, b *ID) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
func sameInspectionKey(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// The walk is bounded by the inspection budget, including retained values. No
// reflection, re-encoding, document clone, or unbounded comparison is required.
func (s *inspectionState) equalInspectionValue(a, b any) (bool, error) {
	if err := s.dec.ReserveValues(1); err != nil {
		return false, err
	}
	switch x := a.(type) {
	case nil:
		return b == nil, nil
	case bool:
		y, ok := b.(bool)
		return ok && x == y, nil
	case int64:
		y, ok := b.(int64)
		return ok && x == y, nil
	case float32:
		y, ok := b.(float32)
		return ok && x == y, nil
	case float64:
		y, ok := b.(float64)
		return ok && x == y, nil
	case string:
		y, ok := b.(string)
		if !ok || len(x) != len(y) {
			return false, nil
		}
		if err := s.dec.ReservePayload(uint64(len(x))); err != nil {
			return false, err
		}
		return x == y, nil
	case []byte:
		y, ok := b.([]byte)
		if !ok || len(x) != len(y) {
			return false, nil
		}
		if err := s.dec.ReservePayload(uint64(len(x))); err != nil {
			return false, err
		}
		return bytes.Equal(x, y), nil
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false, nil
		}
		for i, v := range x {
			equal, err := s.equalInspectionValue(v, y[i])
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false, nil
		}
		for key, v := range x {
			if err := s.dec.ReservePayload(uint64(len(key))); err != nil {
				return false, err
			}
			other, ok := y[key]
			if !ok {
				return false, nil
			}
			equal, err := s.equalInspectionValue(v, other)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil
	default:
		return false, nil
	}
}
