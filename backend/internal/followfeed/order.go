// Package followfeed implements hybrid fanout-on-write / fanout-on-read feeds.
package followfeed

import (
	"container/heap"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"feedsystem_video_go/internal/video"
)

type Cursor struct {
	Time time.Time
	ID   uint
}

func member(t time.Time, id uint) string { return fmt.Sprintf("%020d:%020d", t.UnixMicro(), id) }
func (c Cursor) Member() string {
	if c.Time.IsZero() {
		return ""
	}
	return member(c.Time, c.ID)
}
func EncodeCursor(v *video.Video) string {
	return base64.RawURLEncoding.EncodeToString([]byte(member(v.CreateTime, v.ID)))
}
func parseMember(s string) (Cursor, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 || len(parts[0]) != 20 || len(parts[1]) != 20 {
		return Cursor{}, errors.New("invalid following cursor")
	}
	ts, e1 := strconv.ParseInt(parts[0], 10, 64)
	id, e2 := strconv.ParseUint(parts[1], 10, strconv.IntSize)
	if e1 != nil || e2 != nil || ts <= 0 || ts > 253402300799999999 || id == 0 {
		return Cursor{}, errors.New("invalid following cursor")
	}
	c := Cursor{Time: time.UnixMicro(ts).UTC(), ID: uint(id)}
	if c.Member() != s {
		return Cursor{}, errors.New("invalid following cursor")
	}
	return c, nil
}
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	if len(s) > 128 {
		return Cursor{}, errors.New("invalid following cursor")
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, errors.New("invalid following cursor")
	}
	return parseMember(string(b))
}

func newer(a, b *video.Video) bool {
	if a.CreateTime.Equal(b.CreateTime) {
		return a.ID > b.ID
	}
	return a.CreateTime.After(b.CreateTime)
}

type entry struct {
	v           *video.Video
	source, pos int
}
type mergeHeap []entry

func (h mergeHeap) Len() int            { return len(h) }
func (h mergeHeap) Less(i, j int) bool  { return newer(h[i].v, h[j].v) }
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(entry)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Merge consumes descending sources, de-duplicates IDs, and keeps a bounded heap.
func Merge(sources [][]*video.Video, limit int) []*video.Video {
	h := &mergeHeap{}
	for i, s := range sources {
		if len(s) > 0 {
			*h = append(*h, entry{s[0], i, 0})
		}
	}
	heap.Init(h)
	result := make([]*video.Video, 0, limit)
	seen := make(map[uint]bool)
	for h.Len() > 0 && len(result) < limit {
		e := heap.Pop(h).(entry)
		if !seen[e.v.ID] {
			seen[e.v.ID] = true
			result = append(result, e.v)
		}
		e.pos++
		if e.pos < len(sources[e.source]) {
			e.v = sources[e.source][e.pos]
			heap.Push(h, e)
		}
	}
	return result
}
