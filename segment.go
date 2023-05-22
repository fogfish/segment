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

type segmentStatus int

const (
	segment_present segmentStatus = iota
	segment_swapped
	segment_dirty
)

type keyNotFound[K skiplist.Num] struct{ key K }

func (err *keyNotFound[K]) Error() string    { return fmt.Sprintf("not found: %d", err.key) }
func (err *keyNotFound[K]) NotFound() string { return fmt.Sprintf("%v", err.key) }

type Segment[K skiplist.Num, V any] struct {
	// segment metadata
	id     skiplist.Arc[K]
	status segmentStatus

	// segment engines
	writer Writer[K, V]
	reader Reader[K, V]

	// segment data
	store *skiplist.Map[K, V]
}

func newNode[K skiplist.Num, V any](
	id skiplist.Arc[K],
	writer Writer[K, V],
	reader Reader[K, V],
) *Segment[K, V] {
	return &Segment[K, V]{
		id:     id,
		status: segment_swapped,
		writer: writer,
		reader: reader,
	}
}

func (node *Segment[K, V]) length() int {
	return node.store.Length
}

func (node *Segment[K, V]) split(hd, tl skiplist.Arc[K]) *Segment[K, V] {
	head := &Segment[K, V]{
		id:     hd,
		status: node.status,
		writer: node.writer,
		reader: node.reader,
		store:  node.store,
	}

	node.store = head.store.Split(tl.Lo)
	node.id = tl
	return head
}

func (node *Segment[K, V]) put(key K, val V) (bool, error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return false, err
		}
	}

	isCreated := node.store.Put(key, val)
	if isCreated {
		node.status = segment_dirty
	}

	return isCreated, nil
}

func (node *Segment[K, V]) get(key K) (V, error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return *new(V), err
		}
	}

	val, has := node.store.Get(key)
	if !has {
		return *new(V), &keyNotFound[K]{key}
	}

	return val, nil
}

func (node *Segment[K, V]) cut(key K) (V, error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return *new(V), err
		}
	}

	val, has := node.store.Cut(key)
	if !has {
		return *new(V), &keyNotFound[K]{key}
	}

	return val, nil
}

func (node *Segment[K, V]) values() (skiplist.Iterator[K, V], error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return nil, err
		}
	}

	return skiplist.ForMap(node.store, node.store.Keys()), nil
}

func (node *Segment[K, V]) successors(key K) (skiplist.Iterator[K, V], error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return nil, err
		}
	}

	return skiplist.ForMap(node.store, node.store.Successors(key)), nil
}

func (node *Segment[K, V]) write() error {
	if node.status != segment_dirty {
		return nil
	}

	if err := node.writer.Write(node.id.Hi, node.store); err != nil {
		return err
	}

	node.status = segment_present

	return nil
}

func (node *Segment[K, V]) read() error {
	if node.status != segment_swapped {
		return nil
	}

	if node.reader == nil {
		node.store = skiplist.NewMap[K, V]()
		node.status = segment_present
		return nil
	}

	store, err := node.reader.Read(node.id.Hi)
	if err != nil {
		return err
	}

	node.store = store
	node.status = segment_present
	return nil
}
