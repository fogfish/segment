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

	"github.com/fogfish/golem/trait/pair"
	"github.com/fogfish/segment/internal/sortedmap"
	"github.com/fogfish/skiplist"
)

type Writer[K skiplist.Key, V any] interface {
	Write(*Segment[K, V], *skiplist.Pair[K, V]) error
	WriteMap(map[K]*Segment[K, V], *skiplist.Map[K, V]) error
}

type Reader[K skiplist.Key, V any] interface {
	Read(*Segment[K, V], *skiplist.Map[K, V]) error
	ReadMap(map[K]*Segment[K, V], *skiplist.Map[K, V]) error
}

type keyNotFound[K skiplist.Key] struct{ key K }

func (err *keyNotFound[K]) Error() string    { return fmt.Sprintf("not found: %v", err.key) }
func (err *keyNotFound[K]) NotFound() string { return fmt.Sprintf("%v", err.key) }

type Map[K skiplist.Key, V any] struct {
	writer   Writer[K, V]
	reader   Reader[K, V]
	segments map[K]*Segment[K, V]
	store    *skiplist.Map[K, V]
}

// Level 2 is the level (√B)ⁿ = B, so that block size "predictable"
const L = 2

// Create new instance of segmented map
func New[K skiplist.Key, V any](
	writer Writer[K, V],
	reader Reader[K, V],
	opts ...skiplist.MapConfig[K, V],
) (*Map[K, V], error) {
	store := skiplist.NewMap[K, V](opts...)

	segments := make(map[K]*Segment[K, V])

	// Segment map always kept in memory
	if reader != nil {
		if err := reader.ReadMap(segments, store); err != nil {
			return nil, err
		}
	}

	// Segment map must contain head of skiplist
	if len(segments) == 0 {
		head := store.Head()
		segments[head.Key] = NewSegment(head)
	}

	return &Map[K, V]{
		writer:   writer,
		reader:   reader,
		segments: segments,
		store:    store,
	}, nil
}

// Put key-value pair into map
func (kv *Map[K, V]) Put(key K, val V) (bool, error) {
	segment, err := kv.ensureSegmentForKey(key)
	if err != nil {
		return false, err
	}

	justCreated, pair := kv.store.Put(key, val)
	if !justCreated {
		return justCreated, nil
	}

	if pair.Rank() >= L+1 {
		if err := kv.splitSegment(pair, segment); err != nil {
			return false, err
		}
	}

	return justCreated, nil
}

// Get value
func (kv *Map[K, V]) Get(key K) (V, error) {
	_, err := kv.ensureSegmentForKey(key)
	if err != nil {
		return *new(V), err
	}

	v, node := kv.store.Get(key)
	if node == nil {
		return *new(V), &keyNotFound[K]{key}
	}

	return v, nil
}

// All values of the map
func (kv *Map[K, V]) Values() (pair.Seq[K, V], error) {
	head := kv.store.Head()
	if err := kv.ensureSegment(head); err != nil {
		return nil, err
	}

	return &forMap[K, V]{
		kv:       kv,
		segments: head.NextOn(L),
		values:   head.Next(),
	}, nil
}

// Successor to key
func (kv *Map[K, V]) Successor(key K) (pair.Seq[K, V], error) {
	segment, err := kv.ensureSegmentForKey(key)
	if err != nil {
		return nil, err
	}

	return &forMap[K, V]{
		kv:       kv,
		segments: segment.NextOn(L),
		values:   kv.store.Successor(key),
	}, nil
}

// Sync map to storage
func (kv *Map[K, V]) Sync() error {
	for _, segment := range kv.segments {
		seq := sortedmap.Cut(kv.store, L, segment.Lo)
		if seq != nil {
			if err := kv.writer.Write(segment, seq); err != nil {
				// TODO: rollback seq
				return err
			}
		}
	}

	if err := kv.writer.WriteMap(kv.segments, kv.store); err != nil {
		return err
	}

	return nil
}

// -------------------------------------------------------------------------

func (kv *Map[K, V]) ensureSegmentForKey(key K) (*skiplist.Pair[K, V], error) {
	pair := sortedmap.Predecessor(kv.store, L, key)
	return pair, kv.ensureSegment(pair)
}

func (kv *Map[K, V]) ensureSegment(pair *skiplist.Pair[K, V]) error {
	segment := kv.segments[pair.Key]

	if segment.Swapped {
		fmt.Printf("==> load %v\n", segment.Lo)
		if err := kv.reader.Read(segment, kv.store); err != nil {
			return err
		}
	}

	return nil
}

func (kv *Map[K, V]) splitSegment(pair *skiplist.Pair[K, V], segment *skiplist.Pair[K, V]) error {
	kv.segments[pair.Key] = NewSegment(pair)

	splitted := kv.segments[segment.Key]
	splitted.Hi = pair

	// TODO: split at storage
	fmt.Printf("==> split %v after added %v\n", segment.Key, pair.Key)
	return nil
}

func (kv *Map[K, V]) Debug() {
	// fmt.Printf("==> nodes\n")
	// for _, node := range kv.nodes {
	// 	fmt.Printf("\t%v - %v\n", node.Lo, node.Hi)
	// }
	fmt.Println(kv.store)
}
