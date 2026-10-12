package anycodec

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// Object is a read-only compact Any object. It is returned only by decoders
// explicitly configured with CompactObjects. Keys are sorted and may be shared.
// Child arrays and byte slices must also be treated as read-only.
type Object struct {
	keys   []string
	values []any
}

func (o *Object) Len() int                  { return len(o.keys) }
func (o *Object) Field(i int) (string, any) { return o.keys[i], o.values[i] }
func (o *Object) Get(key string) (any, bool) {
	i := sort.SearchStrings(o.keys, key)
	if i == len(o.keys) || o.keys[i] != key {
		return nil, false
	}
	return o.values[i], true
}
func (o *Object) MarshalJSON() ([]byte, error) {
	out := []byte{'{'}
	for i, key := range o.keys {
		if i != 0 {
			out = append(out, ',')
		}
		k, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		out = append(out, k...)
		out = append(out, ':')
		v, err := json.Marshal(o.values[i])
		if err != nil {
			return nil, err
		}
		out = append(out, v...)
	}
	return append(out, '}'), nil
}

type objectSorter struct{ *Object }

func (o objectSorter) Less(i, j int) bool { return o.keys[i] < o.keys[j] }
func (o objectSorter) Swap(i, j int) {
	o.keys[i], o.keys[j] = o.keys[j], o.keys[i]
	o.values[i], o.values[j] = o.values[j], o.values[i]
}
func (d *Decoder) finishObject(o *Object) *Object {
	sort.Stable(objectSorter{o})
	// Preserve the ordinary decoder's last-value-wins handling of duplicate keys.
	n := 0
	for i, k := range o.keys {
		if n > 0 && o.keys[n-1] == k {
			o.values[n-1] = o.values[i]
			continue
		}
		o.keys[n], o.values[n] = k, o.values[i]
		n++
	}
	if n < len(o.keys) {
		// Do not retain a large backing array for an object with duplicate keys.
		o.keys = append([]string(nil), o.keys[:n]...)
		o.values = append([]any(nil), o.values[:n]...)
	}
	// Bound shape retention per decoder; the ordinary allocation charges cover
	// keys and scratch even when shapes are shared. No global interning cache.
	if n <= 64 {
		var b strings.Builder
		for _, k := range o.keys {
			b.WriteString(strconv.Itoa(len(k)))
			b.WriteByte(':')
			b.WriteString(k)
		}
		signature := b.String()
		if keys, ok := d.shapes[signature]; ok {
			o.keys = keys
		} else if len(d.shapes) < 1024 && len(signature) <= 4096 {
			if d.shapes == nil {
				d.shapes = make(map[string][]string)
			}
			d.shapes[signature] = o.keys
		}
	}
	return o
}
