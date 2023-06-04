//
// Copyright (C) 2023 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/segment
//

package bytes

import (
	"fmt"
	"io"

	"github.com/fogfish/segment"
	"github.com/fogfish/segment/internal/sortedmap"
	"github.com/fogfish/skiplist"
)

// -----------------------------------------------------------------------------------------

// streaming and consumer of segments is efficient, when original list is inverted
type List[K skiplist.Key, V any] struct {
	Item *skiplist.Pair[K, V]
	Next *List[K, V]
}

func NewList[K skiplist.Key, V any](pair *skiplist.Pair[K, V]) (*List[K, V], int) {
	var (
		list   *List[K, V]
		length int
	)

	for e := pair; e != nil; e = e.Next() {
		list = &List[K, V]{Item: e, Next: list}
		length++
	}
	return list, length
}

// -----------------------------------------------------------------------------------------

type Encoder[K skiplist.Key, V any] struct {
	w *WriterTyped
}

func NewEncoder[K skiplist.Key, V any](w io.Writer) *Encoder[K, V] {
	return &Encoder[K, V]{
		w: NewWriterTyped(w),
	}
}

func (c *Encoder[K, V]) Encode(node *segment.Segment[K, V], segment *skiplist.Pair[K, V]) error {
	list, length := NewList(segment)

	// TODO: write header
	c.w.WriteUInt32(uint32(length))
	for e := list; e != nil; e = e.Next {
		c.w.WriteValue(e.Item.Key)
		c.w.WriteValue(e.Item.Value)
	}

	return c.w.Fail
	// seg := &Segment[K, V]{
	// 	Version: version,
	// 	GUID:    guid.G(guid.Clock),
	// 	Lo:      node.Lo.Key,
	// 	Pairs:   pairs,
	// }

	// if node.Hi != nil {
	// 	seg.Hi = &node.Hi.Key
	// }

	// return c.codec.Encode(seg)
}

func (c *Encoder[K, V]) EncodeMap(nodes map[K]*segment.Segment[K, V], kv *skiplist.Map[K, V]) error {
	list, length := NewList(kv.Head())

	c.w.WriteUInt32(uint32(length))
	for e := list; e != nil; e = e.Next {
		node, has := nodes[e.Item.Key]
		if !has {
			return fmt.Errorf("corrupted topology")
		}
		if err := c.encodeMapSegment(node, e.Item); err != nil {
			return err
		}
	}

	return c.w.Fail
}

func (c *Encoder[K, V]) encodeMapSegment(segment *segment.Segment[K, V], pair *skiplist.Pair[K, V]) error {
	c.w.WriteGUID(segment.ID)
	c.w.WriteValue(pair.Value)

	// just pack key + all fingers to seq of K
	size := len(pair.Fingers) + 1
	c.w.WriteUInt32(uint32(size))
	c.w.WriteValue(pair.Key)
	for _, finger := range pair.Fingers {
		if finger != nil {
			c.w.WriteValue(finger.Key)
		} else {
			c.w.WriteValue(*new(K))
		}
	}
	return c.w.Fail
}

// ------------------------------------------------------------------------

type Decoder[K skiplist.Key, V any] struct {
	r *ReaderTyped
}

func NewDecoder[K skiplist.Key, V any](r io.Reader) *Decoder[K, V] {
	return &Decoder[K, V]{
		r: NewReaderTyped(r),
	}
}

func (c *Decoder[K, V]) Decode(node *segment.Segment[K, V], kv *skiplist.Map[K, V]) error {
	var length uint32
	if err := c.r.ReadUInt32(&length); err != nil {
		return err
	}

	for i := 0; i < int(length); i++ {
		var key K
		var val V

		c.r.ReadValue(&key)
		c.r.ReadValue(&val)
		if c.r.Fail == nil {
			sortedmap.Put(kv, segment.L, key, val)
		}
	}

	return nil
}

func (c *Decoder[K, V]) DecodeMap(nodes map[K]*segment.Segment[K, V], kv *skiplist.Map[K, V]) error {
	var length uint32
	if err := c.r.ReadUInt32(&length); err != nil {
		return err
	}

	seq := make([]*segment.Segment[K, V], length)
	for sid := int(length) - 1; sid >= 0; sid-- {
		node, err := c.decodeMapSegment(sid, kv)
		if err != nil {
			return err
		}
		seq[sid] = node
	}

	for sid := length - 1; sid > 0; sid-- {
		node := seq[sid]

		el, _ := kv.Skip(0, node.Lo.Key)
		node.Lo = el
		node.Hi = el.NextOn(segment.L)
		nodes[el.Key] = node
	}

	head := seq[0]
	head.Lo = kv.Head()
	head.Hi = head.Lo.NextOn(segment.L)
	nodes[head.Lo.Key] = head

	return nil
}

func (c *Decoder[K, V]) decodeMapSegment(id int, kv *skiplist.Map[K, V]) (*segment.Segment[K, V], error) {
	var (
		node = &segment.Segment[K, V]{Swapped: true}
		size uint32
		val  V
	)
	c.r.ReadGUID(&node.ID)
	c.r.ReadValue(&val)
	c.r.ReadUInt32(&size)

	seq := make([]K, size)
	for i := 0; i < int(size); i++ {
		c.r.ReadValue(&seq[i])
	}

	if id != 0 {
		sortedmap.Push(kv, seq, val)
	} else {
		sortedmap.PushH(kv, seq)
	}

	node.Lo = &skiplist.Pair[K, V]{Key: seq[0], Value: val}
	return node, c.r.Fail
}
