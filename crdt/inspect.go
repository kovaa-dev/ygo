package crdt

import (
	"context"
	"fmt"
	"sort"

	"github.com/reearth/ygo/encoding"
)

// UpdateInspectionOptions supplies per-operation resource budgets, not document
// schema limits. Zero limits are unlimited; existing decoder limits remain.
type UpdateInspectionOptions struct {
	Context         context.Context
	MaxStructs      uint64
	MaxValues       uint64
	MaxPayloadBytes uint64
	// Reserve runs before charged decoding and inspection scratch allocations.
	Reserve func(uint64) error
}

// InspectedParent describes a nested shared container, from inner to outer.
// Key is nil for a sequence position; a pointer to an empty string is a map key.
type InspectedParent struct {
	Kind string
	Key  *string
}

// InspectedItem describes wire metadata and borrowed content values.
// Content and Parent kinds use stable names: deleted, json, binary, string,
// embed, format, type, any, doc, move; nested types use array, map, text, XML
// names, or unknown. Values/keys are read-only and valid only during the visit.
// Roots carry no concrete type on the wire: validate key-vs-sequence shape too.
type InspectedItem struct {
	ID          ID
	Root        string
	Parents     []InspectedParent
	Key         *string
	ContentKind string
	TypeKind    string
	Values      []any
	Text        string
	Attribute   string // Formatting attribute key, for ContentKind == "format".
	Unresolved  bool
	Orphan      bool
	Pending     bool
	Existing    bool // Exact retransmission of an already retained pending struct.
}

// UpdateInspectionResult reports dependencies whose eventual schema cannot yet
// be determined. Success is NOT proof that unresolved content conforms to a
// schema, nor authorization to drop acknowledged pending content later.
type UpdateInspectionResult struct {
	Items      uint64
	Unresolved uint64
}

// InspectUpdateV1 decodes with the normal V1 parser into an isolated scratch
// document, then visits incoming and previously pending structs while holding
// doc's read lock. It does not mutate doc, integrate, fire observers, or alter
// pending/ACK semantics. Decode, budget, context and visitor failures therefore
// leave doc unchanged. The visitor must not call document methods or mutate any
// supplied values. A successful inspection does NOT make a later ApplyUpdateV1
// atomic: callers must serialize inspect/apply and preserve the existing apply
// error contract. In particular unresolved ancestry must not be called validated.
func InspectUpdateV1(doc *Doc, update []byte, options UpdateInspectionOptions, visit func(InspectedItem) error) (inspectionResult UpdateInspectionResult, resultErr error) {
	var reserveErr error
	originalReserve := options.Reserve
	if originalReserve != nil {
		options.Reserve = func(n uint64) error {
			if reserveErr != nil {
				return reserveErr
			}
			reserveErr = originalReserve(n)
			return reserveErr
		}
	}
	defer func() {
		if reserveErr != nil {
			inspectionResult = UpdateInspectionResult{}
			resultErr = reserveErr
		} else if resultErr != nil && options.Context != nil && options.Context.Err() != nil {
			inspectionResult = UpdateInspectionResult{}
			resultErr = options.Context.Err()
		}
	}()

	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return UpdateInspectionResult{}, err
	}
	budget := &encoding.DecodeBudget{Context: ctx, MaxValues: options.MaxValues, MaxPayloadBytes: options.MaxPayloadBytes, Reserve: options.Reserve}
	dec := encoding.NewDecoderWithBudget(update, budget)
	defer func() {
		if err := dec.BudgetError(); err != nil {
			inspectionResult = UpdateInspectionResult{}
			resultErr = err
		}
	}()
	if err := dec.ReserveAllocation(16384); err != nil {
		return UpdateInspectionResult{}, err
	}
	scratch := New(WithClientID(0))
	defer scratch.Destroy()
	incoming, _, err := decodeStructsV1Bounded(scratch, dec, options.MaxStructs)
	if err != nil {
		return UpdateInspectionResult{}, err
	}
	if dec.HasContent() {
		return UpdateInspectionResult{}, ErrInvalidUpdate
	}
	if err := ctx.Err(); err != nil {
		return UpdateInspectionResult{}, err
	}
	doc.mu.RLock()
	defer doc.mu.RUnlock()
	state := inspectionState{doc: doc, incoming: incoming, pending: make(map[ClientID][]*Item), containers: make(map[*abstractType]*Item), dec: dec}
	for _, items := range incoming {
		if err := ctx.Err(); err != nil {
			return UpdateInspectionResult{}, err
		}
		sort.SliceStable(items, func(i, j int) bool { return items[i].ID.Clock < items[j].ID.Clock })
		for _, item := range items {
			if ct, ok := item.Content.(*ContentType); ok {
				state.containers[ct.Type] = item
			}
		}
	}
	if doc.store.pending != nil {
		if err := dec.ReserveAllocation(uint64(len(doc.store.pending.items)) * 128); err != nil {
			return UpdateInspectionResult{}, err
		}
		if err := dec.ReserveValues(uint64(len(doc.store.pending.items))); err != nil {
			return UpdateInspectionResult{}, err
		}
		for _, item := range doc.store.pending.items {
			state.pending[item.ID.Client] = append(state.pending[item.ID.Client], item)
			if ct, ok := item.Content.(*ContentType); ok {
				state.containers[ct.Type] = item
			}
		}
		for _, items := range state.pending {
			if err := ctx.Err(); err != nil {
				return UpdateInspectionResult{}, err
			}
			sort.SliceStable(items, func(i, j int) bool { return items[i].ID.Clock < items[j].ID.Clock })
		}
	}
	var result UpdateInspectionResult
	for _, group := range []struct {
		items   map[ClientID][]*Item
		pending bool
	}{{incoming, false}, {state.pending, true}} {
		for _, items := range group.items {
			for _, item := range items {
				if err := ctx.Err(); err != nil {
					return result, err
				}
				view, err := state.view(item)
				if err != nil {
					return result, err
				}
				view.Pending = group.pending
				if !group.pending {
					view.Existing, err = state.existingPending(item, view)
					if err != nil {
						return result, err
					}
				}
				result.Items++
				if view.Unresolved {
					result.Unresolved++
				}
				if visit != nil {
					if err := visit(view); err != nil {
						return result, err
					}
				}
			}
		}
	}
	return result, nil
}

type inspectionState struct {
	doc        *Doc
	incoming   map[ClientID][]*Item
	pending    map[ClientID][]*Item
	containers map[*abstractType]*Item
	dec        *encoding.Decoder
}

func (s *inspectionState) find(id ID) *Item {
	if it := s.doc.store.Find(id); it != nil {
		return it
	}
	for _, groups := range []map[ClientID][]*Item{s.incoming, s.pending} {
		items := groups[id.Client]
		i := sort.Search(len(items), func(i int) bool { return items[i].ID.Clock > id.Clock }) - 1
		if i >= 0 && id.Clock-items[i].ID.Clock < uint64(items[i].Content.Len()) {
			return items[i]
		}
	}
	return nil
}

// parent resolves anchors without splitting or modifying integrated/pending items.
func (s *inspectionState) parent(item *Item) (*abstractType, *string, bool, error) {
	key := item.ParentSub
	seen := make(map[*Item]bool)
	for item != nil {
		if err := s.dec.ReserveValues(1); err != nil {
			return nil, nil, false, err
		}
		if seen[item] {
			return nil, nil, false, fmt.Errorf("%w: cyclic ancestry", ErrInvalidUpdate)
		}
		seen[item] = true
		if key == nil {
			key = item.ParentSub
		}
		if item.Parent != nil {
			return item.Parent, key, false, nil
		}
		if item.parentID != nil {
			container := s.find(*item.parentID)
			if container == nil {
				return nil, key, true, nil
			}
			if ct, ok := container.Content.(*ContentType); ok {
				return ct.Type, key, false, nil
			}
			return nil, key, false, nil
		}
		var anchor *ID
		if item.Origin != nil {
			anchor = item.Origin
		} else if item.OriginRight != nil {
			anchor = item.OriginRight
		}
		if anchor == nil {
			return nil, key, false, nil
		}
		item = s.find(*anchor)
		if item == nil {
			return nil, key, true, nil
		}
	}
	return nil, key, true, nil
}

func (s *inspectionState) view(item *Item) (InspectedItem, error) {
	view := InspectedItem{ID: item.ID}
	switch c := item.Content.(type) {
	case *ContentDeleted:
		view.ContentKind = "deleted"
	case *ContentJSON:
		view.ContentKind = "json"
		view.Values = c.Vals
	case *ContentBinary:
		view.ContentKind = "binary"
	case *ContentString:
		view.ContentKind = "string"
		view.Text = c.Str
	case *ContentEmbed:
		view.ContentKind = "embed"
		view.Values = []any{c.Val}
	case *ContentFormat:
		view.ContentKind = "format"
		view.Attribute = c.Key
		view.Values = []any{c.Val}
	case *ContentType:
		view.ContentKind = "type"
		view.TypeKind = inspectionType(c.Type)
	case *ContentAny:
		view.ContentKind = "any"
		view.Values = c.Vals
	case *ContentDoc:
		view.ContentKind = "doc"
	case *ContentMove:
		view.ContentKind = "move"
	default:
		return view, ErrInvalidUpdate
	}
	parent, key, missing, err := s.parent(item)
	if err != nil {
		return view, err
	}
	view.Key = key
	view.Unresolved = missing
	seen := make(map[*abstractType]bool)
	for parent != nil {
		if err := s.dec.ReserveValues(1); err != nil {
			return view, err
		}
		if seen[parent] {
			return view, fmt.Errorf("%w: cyclic parent", ErrInvalidUpdate)
		}
		seen[parent] = true
		root := parent.name != ""
		if !root && parent.doc != nil {
			if empty := parent.doc.share[""]; empty != nil {
				root = empty.baseType() == parent
			}
		}
		if root {
			view.Root = parent.name
			return view, nil
		}
		container := parent.item
		if container == nil {
			container = s.containers[parent]
		}
		if container == nil {
			view.Unresolved = true
			return view, nil
		}
		outer, containerKey, unresolved, err := s.parent(container)
		if err != nil {
			return view, err
		}
		view.Parents = append(view.Parents, InspectedParent{Kind: inspectionType(parent), Key: containerKey})
		view.Unresolved = view.Unresolved || unresolved
		parent = outer
	}
	view.Orphan = !view.Unresolved
	return view, nil
}

func inspectionType(t *abstractType) string {
	switch t.owner.(type) {
	case *YArray:
		return "array"
	case *YMap:
		return "map"
	case *YText:
		return "text"
	case *YXmlElement:
		return "xml-element"
	case *YXmlFragment:
		return "xml-fragment"
	case *YXmlText:
		return "xml-text"
	default:
		return "unknown"
	}
}
