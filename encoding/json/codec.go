//
// Copyright (C) 2023 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/segment
//

package json

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/fogfish/guid/v2"
	"github.com/fogfish/segment"
	"github.com/fogfish/segment/internal/sortedmap"
	"github.com/fogfish/skiplist"
)

const version = "v1"

type Segment[K skiplist.Key, V any] struct {
	Version string  `json:"version"`
	GUID    guid.K  `json:"guid"`
	Lo      K       `json:"lo"`
	Hi      *K      `json:"hi,omitempty"`
	Pairs   map[K]V `json:"pairs"`
}

type Node[K skiplist.Key, V any] struct {
	ID  guid.K `json:"id"`
	Seq []K    `json:"seq"`
	Val V      `json:"val"`
}

// ------------------------------------------------------------------------

type Encoder[K skiplist.Key, V any] struct {
	codec *json.Encoder
}

func NewEncoder[K skiplist.Key, V any](w io.Writer) *Encoder[K, V] {
	return &Encoder[K, V]{
		codec: json.NewEncoder(w),
	}
}

func (c *Encoder[K, V]) Encode(node *segment.Segment[K, V], segment *skiplist.Pair[K, V]) error {
	pairs := map[K]V{}
	for e := segment; e != nil; e = e.Next() {
		pairs[e.Key] = e.Value
	}

	seg := &Segment[K, V]{
		Version: version,
		GUID:    guid.G(guid.Clock),
		Lo:      node.Lo.Key,
		Pairs:   pairs,
	}

	if node.Hi != nil {
		seg.Hi = &node.Hi.Key
	}

	return c.codec.Encode(seg)
}

func (c *Encoder[K, V]) EncodeMap(nodes map[K]*segment.Segment[K, V], kv *skiplist.Map[K, V]) error {
	nn := make([]Node[K, V], 0)
	for e := kv.Head(); e != nil; e = e.Next() {
		node, has := nodes[e.Key]
		if !has {
			return fmt.Errorf("corrupted topology")
		}

		seq := make([]K, len(e.Fingers)+1)
		seq[0] = e.Key
		for i, f := range e.Fingers {
			if f != nil {
				seq[i+1] = f.Key
			}
		}

		nn = append(nn, Node[K, V]{
			ID:  node.ID,
			Seq: seq,
			Val: e.Value,
		})
	}

	return c.codec.Encode(nn)
}

// ------------------------------------------------------------------------

type Decoder[K skiplist.Key, V any] struct {
	codec *json.Decoder
}

func NewDecoder[K skiplist.Key, V any](r io.Reader) *Decoder[K, V] {
	return &Decoder[K, V]{
		codec: json.NewDecoder(r),
	}
}

func (c *Decoder[K, V]) Decode(node *segment.Segment[K, V], kv *skiplist.Map[K, V]) error {
	var seg *Segment[K, V]

	if err := c.codec.Decode(&seg); err != nil {
		return err
	}

	for key, val := range seg.Pairs {
		sortedmap.Put(kv, segment.L, key, val)
	}

	return nil
}

func (c *Decoder[K, V]) DecodeMap(nodes map[K]*segment.Segment[K, V], kv *skiplist.Map[K, V]) error {
	nn := make([]Node[K, V], 0)
	if err := c.codec.Decode(&nn); err != nil {
		return err
	}

	for i := len(nn) - 1; i > 0; i-- {
		sortedmap.Push(kv, nn[i].Seq, nn[i].Val)
	}
	sortedmap.PushH(kv, nn[0].Seq)

	for i := len(nn) - 1; i > 0; i-- {
		el, _ := kv.Skip(0, nn[i].Seq[0])
		node := &segment.Segment[K, V]{
			ID:      nn[i].ID,
			Lo:      el,
			Hi:      el.NextOn(segment.L),
			Swapped: true,
		}
		nodes[el.Key] = node
	}
	head := kv.Head()
	nodes[head.Key] = &segment.Segment[K, V]{
		ID:      nn[0].ID,
		Lo:      head,
		Hi:      head.NextOn(segment.L),
		Swapped: true,
	}

	return nil
}
