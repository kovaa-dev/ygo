package crdt

import (
	"bytes"
	"cmp"
	"encoding/json"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/reearth/ygo/encoding"
	"github.com/reearth/ygo/internal/anycodec"
)

// pendingBudget distinguishes a large complete update from a genuinely oversized
// pending queue before the decoder materializes the rest of its items. The cheap
// path needs no scan. At the cap, one wire pass and a dependency worklist follow
// clocks without constructing items, content values, or shared types.
// References merely being present in the message is insufficient: their own
// dependencies must be reachable too (including same-client predecessors).
type pendingBudget struct {
	initial   StateVector
	update    []byte
	remaining int
	v2        bool
	checked   bool
}

func newPendingBudget(doc *Doc, initial StateVector, update []byte, v2 bool) pendingBudget {
	remaining := doc.maxPendingItemsLimit()
	if doc.store.pending != nil {
		remaining -= len(doc.store.pending.items)
	}
	return pendingBudget{initial: initial, update: update, remaining: remaining, v2: v2}
}

func (b *pendingBudget) check(count int) error {
	if b.checked || count < b.remaining {
		return nil
	}
	known := make(StateVector, len(b.initial))
	for client, clock := range b.initial {
		known[client] = clock
	}
	s := newPendingScanner(b.update, b.v2)
	clients := s.uint()
	if clients > maxV2Items {
		return ErrInvalidUpdate
	}
	var nodes []pendingSpan
	var total uint64
	for i := uint64(0); i < clients && s.err == nil; i++ {
		n := s.uint()
		total += n
		if total > maxV2Items {
			return ErrInvalidUpdate
		}
		client, clock := s.client(), s.uint()
		existingEnd := known.Clock(client)
		for j := uint64(0); j < n && s.err == nil; j++ {
			length, skip, deps, numDeps := s.item()
			end := clock + length
			if end < clock {
				return ErrInvalidUpdate
			}
			// Skip structs advance only the wire cursor, never known clocks.
			if !skip && end > existingEnd {
				ready := clock <= existingEnd
				for _, dep := range deps[:numDeps] {
					if dep.Clock >= known.Clock(dep.Client) {
						ready = false
					}
				}
				if ready {
					known[client] = end
					existingEnd = end
				} else {
					// A long same-client tail blocked on one parent needs one
					// tuple, not one allocation per item. Equal lengths retain
					// exact item counts if another group covers only a prefix.
					if len(nodes) > 0 && nodes[len(nodes)-1].canExtend(client, clock, length, deps, numDeps) {
						nodes[len(nodes)-1].end = end
						nodes[len(nodes)-1].count++
					} else {
						nodes = append(nodes, pendingSpan{client: client, clock: clock, end: end, length: length, deps: deps, numDeps: numDeps, count: 1})
					}
				}
			}
			clock = end
		}
	}
	if s.err != nil {
		return wrapUpdateErr(s.err)
	}
	if pendingSpanCount(nodes, known) > b.remaining {
		resolvePendingSpans(nodes, known)
		if pendingSpanCount(nodes, known) > b.remaining {
			return ErrInvalidUpdate
		}
	}
	b.checked = true
	return nil
}

// pendingSpan contains only dependency metadata. Consecutive equal-length items
// with identical dependencies share a tuple; no CRDT content is retained.
type pendingSpan struct {
	client             ClientID
	clock, end, length uint64
	deps               [3]ID
	numDeps, count     int
	waiting            int
	queued, done       bool
}

func (n *pendingSpan) canExtend(client ClientID, clock, length uint64, deps [3]ID, numDeps int) bool {
	return n.length > 0 && n.client == client && n.end == clock &&
		n.length == length && n.numDeps == numDeps && n.deps == deps
}

// pendingSpanCount counts wire structs beyond each contiguous known frontier.
func pendingSpanCount(nodes []pendingSpan, known StateVector) int {
	count := 0
	for _, n := range nodes {
		clock := known.Clock(n.client)
		if clock >= n.end {
			continue
		}
		remaining := n.count
		if clock > n.clock && n.length > 0 {
			remaining -= int((clock - n.clock) / n.length)
		}
		count += remaining
	}
	return count
}

// Each dependency is registered once and consumed once when its client's clock
// advances. Sorting waiters replaces repeated full-wire scans; queued tuples are
// processed at most once, even when another client group covers their range.
func resolvePendingSpans(nodes []pendingSpan, known StateVector) {
	type waiter struct {
		client ClientID
		clock  uint64 // required next clock, inclusive
		node   int
		covers bool
	}
	var waits []waiter
	queue := make([]int, 0, len(nodes))
	for i := range nodes {
		n := &nodes[i]
		if known.Clock(n.client) >= n.end {
			n.done = true
			continue
		}
		// Coverage can make a duplicate/overlapping tuple irrelevant even
		// while its explicit dependencies are still missing.
		waits = append(waits, waiter{client: n.client, clock: n.end, node: i, covers: true})
		if n.clock > known.Clock(n.client) {
			waits = append(waits, waiter{client: n.client, clock: n.clock, node: i})
			n.waiting++
		}
		for _, dep := range n.deps[:n.numDeps] {
			if dep.Clock >= known.Clock(dep.Client) {
				waits = append(waits, waiter{client: dep.Client, clock: dep.Clock + 1, node: i})
				n.waiting++
			}
		}
		if n.waiting == 0 {
			n.queued = true
			queue = append(queue, i)
		}
	}
	slices.SortFunc(waits, func(a, b waiter) int {
		if a.client != b.client {
			return cmp.Compare(a.client, b.client)
		}
		return cmp.Compare(a.clock, b.clock)
	})
	type interval struct{ next, end int }
	byClient := make(map[ClientID]interval)
	for i := 0; i < len(waits); {
		end := i + 1
		for end < len(waits) && waits[end].client == waits[i].client {
			end++
		}
		byClient[waits[i].client] = interval{next: i, end: end}
		i = end
	}
	for head := 0; head < len(queue); head++ {
		n := &nodes[queue[head]]
		if n.done {
			continue
		}
		n.done = true
		if known.Clock(n.client) >= n.end {
			continue
		}
		known[n.client] = n.end
		window := byClient[n.client]
		for window.next < window.end && waits[window.next].clock <= n.end {
			w := waits[window.next]
			window.next++
			target := &nodes[w.node]
			if target.done || target.queued {
				continue
			}
			if !w.covers {
				target.waiting--
			}
			if w.covers || target.waiting == 0 {
				target.queued = true
				queue = append(queue, w.node)
			}
		}
		byClient[n.client] = window
	}
}

// Only decoder cursors and scalar clocks survive a scan. V1 reads the caller's
// buffer; V2 uses its normal column decoder without building a key dictionary
// or decoding Any/JSON values into object trees.
type pendingScanner struct {
	rest *encoding.Decoder
	v2   *v2Decoder
	keys int
	err  error
}

func newPendingScanner(update []byte, v2 bool) *pendingScanner {
	s := &pendingScanner{}
	if v2 {
		s.v2, s.err = newV2Decoder(update)
		if s.err == nil {
			s.rest = s.v2.restDec
		}
	} else {
		s.rest = encoding.NewDecoder(update)
	}
	return s
}

func (s *pendingScanner) uint() uint64 {
	if s.err != nil {
		return 0
	}
	v, err := s.rest.ReadVarUint()
	s.err = err
	return v
}
func (s *pendingScanner) byte() byte {
	if s.err != nil {
		return 0
	}
	v, err := s.rest.ReadUint8()
	s.err = err
	return v
}
func (s *pendingScanner) client() ClientID {
	if s.v2 == nil {
		return ClientID(s.uint())
	}
	if s.err != nil {
		return 0
	}
	v, err := s.v2.readClient()
	s.err = err
	return v
}
func (s *pendingScanner) id(right bool) ID {
	if s.v2 == nil {
		return ID{Client: s.client(), Clock: s.uint()}
	}
	if s.err != nil {
		return ID{}
	}
	var id ID
	if right {
		id, s.err = s.v2.readRightID()
	} else {
		id, s.err = s.v2.readLeftID()
	}
	return id
}
func (s *pendingScanner) length() uint64 {
	if s.v2 == nil {
		return s.uint()
	}
	if s.err != nil {
		return 0
	}
	v, err := s.v2.readLen()
	s.err = err
	return uint64(v)
}
func (s *pendingScanner) bytes() {
	if s.err == nil {
		_, s.err = s.rest.ReadVarBytes()
	}
}
func (s *pendingScanner) any() {
	if s.err == nil {
		rest := s.rest.RemainingBytes()
		consumed, err := anycodec.Skip(rest)
		// Keep the existing pointer (also v2.restDec), resetting its buffer to
		// the unread suffix. This advances without re-reading every skipped byte.
		*s.rest = *encoding.NewDecoder(rest[consumed:])
		s.err = err
	}
}

func (s *pendingScanner) text() uint64 {
	if s.err != nil {
		return 0
	}
	var n uint64
	if s.v2 != nil {
		var str string
		str, s.err = s.v2.readString()
		for _, r := range str {
			n++
			if r > 0xffff {
				n++
			}
		}
	} else {
		var raw []byte
		raw, s.err = s.rest.ReadVarBytes()
		if s.err == nil && !utf8.Valid(raw) {
			s.err = encoding.ErrInvalidUTF8
		}
		for len(raw) > 0 {
			r, size := utf8.DecodeRune(raw)
			raw = raw[size:]
			n++
			if r > 0xffff {
				n++
			}
		}
	}
	return n
}
func (s *pendingScanner) key() {
	if s.v2 == nil {
		s.text()
		return
	}
	if s.err != nil {
		return
	}
	var index int64
	index, s.err = s.v2.keyClockDec.Read()
	if index < 0 {
		s.err = ErrInvalidUpdate
	}
	if s.err == nil && index >= int64(s.keys) {
		s.text()
		s.keys++
	}
}
func (s *pendingScanner) item() (length uint64, skip bool, deps [3]ID, numDeps int) {
	var info byte
	if s.err != nil {
		return
	}
	if s.v2 == nil {
		info = s.byte()
	} else {
		info, s.err = s.v2.readInfo()
	}
	tag := info & 0x1f
	if tag == 0 {
		return s.length(), false, deps, 0
	}
	if tag == 10 {
		return s.uint(), true, deps, 0
	}
	if info&flagHasOrigin != 0 {
		deps[numDeps] = s.id(false)
		numDeps++
	}
	if info&flagHasRightOrigin != 0 {
		deps[numDeps] = s.id(true)
		numDeps++
	}
	if numDeps == 0 {
		var named bool
		if s.v2 == nil {
			named = s.byte() == 1
		} else if s.err == nil {
			named, s.err = s.v2.readParentInfo()
		}
		if named {
			s.text()
		} else {
			deps[0] = s.id(false)
			numDeps++
		}
		if info&flagHasParentSub != 0 {
			s.text()
		}
	}
	return s.content(tag), false, deps, numDeps
}
func (s *pendingScanner) content(tag byte) uint64 {
	if s.err != nil {
		return 0
	}
	switch tag {
	case wireDeleted:
		return s.length()
	case wireJSON, wireAny:
		n := s.length()
		if (s.v2 != nil && n > maxV2Items) || (s.v2 == nil && n > uint64(s.rest.Remaining())) {
			s.err = ErrInvalidUpdate
			return 0
		}
		if tag == wireJSON && s.v2 == nil {
			// Match decodeContent's whole-item JSON-first legacy fallback,
			// without constructing the values or their nested object trees.
			probe := *s.rest
			jsonErr := skipJSONVals(&probe, n)
			if jsonErr == nil {
				*s.rest = probe
			} else if n > 0 && isAnyTag(s.rest.RemainingBytes()[0]) {
				for i := uint64(0); i < n && s.err == nil; i++ {
					s.any()
				}
				if s.err != nil {
					s.err = jsonErr
				}
			} else {
				s.err = jsonErr
			}
		} else {
			for i := uint64(0); i < n && s.err == nil; i++ {
				if tag == wireJSON {
					s.text()
				} else {
					s.any()
				}
			}
		}
		return n
	case wireBinary:
		s.bytes()
	case wireString:
		return s.text()
	case wireEmbed:
		if s.v2 == nil {
			s.text()
		} else {
			s.any()
		}
	case wireFormat:
		s.key()
		if s.v2 == nil {
			s.text()
		} else {
			s.any()
		}
	case wireType:
		var ref byte
		if s.v2 == nil {
			ref = s.byte()
		} else {
			ref, s.err = s.v2.readTypeRef()
		}
		if ref == 3 || ref == 5 {
			s.key()
		}
	case wireDoc:
		if s.v2 == nil {
			s.bytes()
		} else {
			s.text()
		}
		s.any()
	case wireMove:
		s.uint()
		s.uint()
		s.uint()
	default:
		s.err = ErrInvalidUpdate
	}
	return 1
}

// skipJSONVals validates V1 JSON text without allocating decoded values.
func skipJSONVals(dec *encoding.Decoder, n uint64) error {
	for i := uint64(0); i < n; i++ {
		raw, err := dec.ReadVarBytes()
		if err != nil {
			return err
		}
		if !utf8.Valid(raw) {
			return encoding.ErrInvalidUTF8
		}
		if bytes.Equal(raw, []byte("undefined")) {
			continue
		}
		if !json.Valid(raw) {
			return ErrInvalidUpdate
		}
		// Unmarshal rejects numbers outside float64's range. Preserve that
		// decision too: it determines the legacy fallback for ambiguous
		// 116–127-byte strings. JSON syntax is already validated above.
		for j := 0; j < len(raw); j++ {
			if raw[j] == '"' {
				for j++; raw[j] != '"'; j++ {
					if raw[j] == '\\' {
						j++
					}
				}
			} else if raw[j] == '-' || raw[j] >= '0' && raw[j] <= '9' {
				start := j
				for j < len(raw) && (raw[j] >= '0' && raw[j] <= '9' || raw[j] == '-' || raw[j] == '+' || raw[j] == '.' || raw[j] == 'e' || raw[j] == 'E') {
					j++
				}
				if _, err := strconv.ParseFloat(string(raw[start:j]), 64); err != nil {
					return ErrInvalidUpdate
				}
				j--
			}
		}
	}
	return nil
}
