package crdt

import (
	"encoding/json"
	"errors"

	"github.com/reearth/ygo/encoding"
)

// ErrSemanticReadUnsupported rejects values outside JSON and the supported shared
// JSON types before invoking an arbitrary marshaler. It never drops a value.
var ErrSemanticReadUnsupported = errors.New("crdt: unsupported budgeted semantic value")

// KeysWithBudget admits the key slice before allocation, under the same read
// lock as the traversal. String data stays owned by the document.
func (m *YMap) KeysWithBudget(budget *encoding.EncodeBudget) ([]string, error) {
	if m.doc != nil {
		m.doc.mu.RLock()
		defer m.doc.mu.RUnlock()
	}
	meter := encoding.NewEncoderWithBudget(budget)
	count := 0
	if m.detached() {
		count = len(m.prelimKeys)
	} else {
		for _, item := range m.itemMap {
			if !meter.Work(1) {
				return nil, meter.Err()
			}
			if !item.Deleted {
				count++
			}
		}
	}
	if !meter.ReserveAllocation(uint64(count)*32 + 64) {
		return nil, meter.Err()
	}
	keys := make([]string, 0, count)
	if m.detached() {
		keys = append(keys, m.prelimKeys...)
	} else {
		for key, item := range m.itemMap {
			if !meter.Work(1) {
				return nil, meter.Err()
			}
			if !item.Deleted {
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}

// AdmitJSONValue charges traversal and JSON container/string workspace before
// callers marshal a canonical JSON-shaped value. It is allocation accounting,
// not an exact RSS bound. The caller owns release of the reservation.
func AdmitJSONValue(value any, budget *encoding.EncodeBudget) error {
	meter := encoding.NewEncoderWithBudget(budget)
	if err := admitSemanticValue(value, meter); err != nil {
		return err
	}
	return meter.Err()
}
func admitSemanticValue(value any, meter *encoding.Encoder) error {
	if !meter.ReserveAllocation(32) {
		return meter.Err()
	}
	stack := []any{value}
	for len(stack) != 0 {
		value = stack[len(stack)-1]
		stack[len(stack)-1] = nil
		stack = stack[:len(stack)-1]
		if !meter.Work(1) || !meter.ReserveAllocation(128) {
			return meter.Err()
		}
		switch v := value.(type) {
		case nil, bool, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		case json.Number:
			if !meter.ReserveAllocation(uint64(len(v)) * 2) {
				return meter.Err()
			}
		case string:
			if !meter.ReserveAllocation(uint64(len(v)) * 12) {
				return meter.Err()
			}
		case []byte:
			if !meter.ReserveAllocation(uint64(len(v)) * 4) {
				return meter.Err()
			}
		case []any:
			if !meter.ReserveAllocation(uint64(len(v)) * 64) {
				return meter.Err()
			}
			stack = append(stack, v...)
		case map[string]any:
			if !meter.ReserveAllocation(uint64(len(v)) * 64) {
				return meter.Err()
			}
			for key, item := range v {
				if !meter.ReserveAllocation(uint64(len(key))*12 + 96) {
					return meter.Err()
				}
				stack = append(stack, item)
			}
		case Attributes:
			if !meter.ReserveAllocation(uint64(len(v)) * 64) {
				return meter.Err()
			}
			for key, item := range v {
				if !meter.ReserveAllocation(uint64(len(key))*12 + 96) {
					return meter.Err()
				}
				stack = append(stack, item)
			}
		default:
			return ErrSemanticReadUnsupported
		}
	}
	return nil
}

// ToDeltaWithBudget retains exactly ToDelta's coalescing/formatting behavior,
// admitting its temporary strings, operations and attribute copies first.
func (txt *YText) ToDeltaWithBudget(budget *encoding.EncodeBudget) ([]Delta, error) {
	if txt.doc != nil {
		txt.doc.mu.RLock()
		defer txt.doc.mu.RUnlock()
	}
	meter := encoding.NewEncoderWithBudget(budget)
	if err := admitSemanticText(txt, meter); err != nil {
		return nil, err
	}
	return txt.toDeltaLocked(), nil
}
func admitSemanticText(txt *YText, meter *encoding.Encoder) error {
	if !meter.ReserveAllocation(256) {
		return meter.Err()
	}
	attrs := Attributes{}
	for item := txt.start; item != nil; item = item.Right {
		if !meter.Work(1) {
			return meter.Err()
		}
		if item.Deleted {
			continue
		}
		switch c := item.Content.(type) {
		case *ContentFormat:
			if c.Val == nil {
				delete(attrs, c.Key)
			} else {
				if _, exists := attrs[c.Key]; !exists && !meter.ReserveAllocation(uint64(len(c.Key))+96) {
					return meter.Err()
				}
				attrs[c.Key] = c.Val
				if err := admitSemanticValue(c.Val, meter); err != nil {
					return err
				}
			}
		case *ContentString:
			if err := admitSemanticValue(attrs, meter); err != nil {
				return err
			}
			if !meter.ReserveAllocation(uint64(len(c.Str))*12 + uint64(len(attrs))*128 + 512) {
				return meter.Err()
			}
		case *ContentEmbed:
			if err := admitSemanticValue(attrs, meter); err != nil {
				return err
			}
			if !meter.ReserveAllocation(uint64(len(attrs))*128 + 512) {
				return meter.Err()
			}
			if err := admitSemanticValue(c.Val, meter); err != nil {
				return err
			}
		}
	}
	return meter.Err()
}

// ToJSONWithBudget charges recursively unwrapped arrays/maps/text before their
// standard JSON conversion. Unknown shared types are refused, never omitted.
func (a *YArray) ToJSONWithBudget(budget *encoding.EncodeBudget) ([]byte, error) {
	if a.doc != nil {
		a.doc.mu.RLock()
		defer a.doc.mu.RUnlock()
	}
	meter := encoding.NewEncoderWithBudget(budget)
	if err := admitSemanticArray(a, meter); err != nil {
		return nil, err
	}
	return json.Marshal(a.toSliceLocked())
}
func admitSemanticArray(a *YArray, meter *encoding.Encoder) error {
	if !meter.Work(1) || !meter.ReserveAllocation(uint64(a.length)*32+128) {
		return meter.Err()
	}
	if a.detached() {
		for _, value := range a.prelim {
			if err := admitSemanticValue(value, meter); err != nil {
				return err
			}
		}
		return nil
	}
	for item := a.start; item != nil; item = item.Right {
		if !meter.Work(1) {
			return meter.Err()
		}
		if item.Deleted {
			continue
		}
		switch c := item.Content.(type) {
		case *ContentAny:
			for _, value := range c.Vals {
				if err := admitSemanticValue(value, meter); err != nil {
					return err
				}
			}
		case *ContentJSON:
			for _, value := range c.Vals {
				if err := admitSemanticValue(value, meter); err != nil {
					return err
				}
			}
		case *ContentEmbed:
			if err := admitSemanticValue(c.Val, meter); err != nil {
				return err
			}
		case *ContentType:
			if err := admitSemanticType(c, meter); err != nil {
				return err
			}
		}
	}
	return nil
}
func admitSemanticType(c *ContentType, meter *encoding.Encoder) error {
	if c == nil || c.Type == nil {
		return nil
	}
	if !meter.Work(1) || !meter.ReserveAllocation(128) {
		return meter.Err()
	}
	switch value := c.Type.owner.(type) {
	case *YArray:
		return admitSemanticArray(value, meter)
	case *YText:
		return admitSemanticText(value, meter)
	case *YMap:
		for key, item := range value.itemMap {
			if !meter.Work(1) {
				return meter.Err()
			}
			if item.Deleted {
				continue
			}
			if !meter.ReserveAllocation(uint64(len(key))*12 + 128) {
				return meter.Err()
			}
			switch itemValue := item.Content.(type) {
			case *ContentAny:
				for _, v := range itemValue.Vals {
					if err := admitSemanticValue(v, meter); err != nil {
						return err
					}
				}
			case *ContentType:
				if err := admitSemanticType(itemValue, meter); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return ErrSemanticReadUnsupported
	}
}
