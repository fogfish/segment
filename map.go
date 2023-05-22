//
// Copyright (C) 2023 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/segment
//

package segment

import (
	"fmt"

	"github.com/fogfish/skiplist"
)

// TODO
//  - configurable caching behavior

type Writer[K skiplist.Num, V any] interface {
	WriteMeta(*skiplist.GF2[K]) error
	Write(K, *skiplist.Map[K, V]) error
}

type Reader[K skiplist.Num, V any] interface {
	ReadMeta() (*skiplist.GF2[K], error)
	Read(K) (*skiplist.Map[K, V], error)
}

// Persistent skiplist data structure
type Map[K skiplist.Num, V any] struct {
	// engines
	writer Writer[K, V]
	reader Reader[K, V]

	// topology
	gf2 *skiplist.GF2[K]

	// data of map
	capacity int
	segments map[K]*Segment[K, V]
}

func New[K skiplist.Num, V any](
	capacity int,
	writer Writer[K, V],
	reader Reader[K, V],
) (kv *Map[K, V], err error) {
	kv = &Map[K, V]{
		capacity: capacity,
		reader:   reader,
		writer:   writer,
	}

	if kv.reader != nil {
		kv.gf2, err = kv.reader.ReadMeta()
		if err != nil {
			return nil, err
		}
	}

	if kv.gf2 == nil {
		kv.gf2 = skiplist.NewGF2[K]()
	}

	kv.segments = make(map[K]*Segment[K, V])
	arcs := skiplist.ForGF2(kv.gf2, kv.gf2.Keys())

	for has := arcs != nil; has; has = arcs.Next() {
		id := arcs.Value()
		kv.segments[id.Hi] = newNode(id, kv.writer, kv.reader)
	}

	return kv, nil
}

func (kv *Map[K, V]) segmentForKey(key K) (*Segment[K, V], error) {
	arc, has := kv.gf2.Get(key)
	if !has {
		return nil, fmt.Errorf("non-continuos field of segments")
	}

	segment, has := kv.segments[arc.Hi]
	if !has {
		return nil, fmt.Errorf("non-continuos field of segments")
	}

	if err := segment.read(); err != nil {
		return nil, err
	}

	return segment, nil
}

func (kv *Map[K, V]) Put(key K, val V) (bool, error) {
	node, err := kv.segmentForKey(key)
	if err != nil {
		return false, err
	}

	isCreated, err := node.put(key, val)
	if err != nil {
		return false, err
	}

	if node.length() <= kv.capacity || node.id.Lo == node.id.Hi {
		return isCreated, nil
	}

	kv.split(key)

	return isCreated, nil
}

func (kv *Map[K, V]) split(addr K) {
	hd, tl := kv.gf2.Add(addr)
	if hd.Lo == hd.Hi && hd.Hi == tl.Lo && tl.Lo == tl.Hi {
		// Not possible to split. Node overloaded
		return
	}

	tail := kv.segments[tl.Hi]
	head := tail.split(hd, tl)
	kv.segments[hd.Hi] = head

	if tail.length() > kv.capacity || head.length() > kv.capacity {
		kv.split(addr)
	}
}

func (kv *Map[K, V]) Get(key K) (V, error) {
	node, err := kv.segmentForKey(key)
	if err != nil {
		return *new(V), err
	}

	val, err := node.get(key)
	if err != nil {
		return val, err
	}

	return val, nil
}

func (kv *Map[K, V]) Cut(key K) (V, error) {
	node, err := kv.segmentForKey(key)
	if err != nil {
		return *new(V), err
	}

	val, err := node.cut(key)
	if err != nil {
		return val, err
	}

	return val, nil
}

func (kv *Map[K, V]) Values() (seq skiplist.Iterator[K, V], err error) {
	seq = skiplist.Join(
		skiplist.ForGF2(kv.gf2, kv.gf2.Keys()),
		func(addr K, arc skiplist.Arc[K]) (subseq skiplist.Iterator[K, V]) {
			if err != nil {
				return nil
			}

			segment := kv.segments[addr]
			subseq, err = segment.values()
			return
		},
	)

	return
}

func (kv *Map[K, V]) Successors(key K) (seq skiplist.Iterator[K, V], err error) {
	seq = skiplist.Join(
		skiplist.ForGF2(kv.gf2, kv.gf2.Successors(key)),
		func(addr K, arc skiplist.Arc[K]) (subseq skiplist.Iterator[K, V]) {
			if err != nil {
				return nil
			}

			fmt.Printf("==> over%v\n", arc)
			node := kv.segments[addr]
			subseq, err = node.successors(key)
			return
		},
	)

	return
}

func (kv *Map[K, V]) Sync() error {
	if kv.writer == nil {
		return fmt.Errorf("no writer")
	}

	for _, segment := range kv.segments {
		if err := segment.write(); err != nil {
			return err
		}
	}

	if err := kv.writer.WriteMeta(kv.gf2); err != nil {
		return err
	}

	return nil
}
