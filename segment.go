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

type Segment struct {
	id     skiplist.Arc[Addr]
	status segmentStatus
	store  *skiplist.Map[Addr, string]
	writer Writer
	reader Reader
}

func newNode(id skiplist.Arc[Addr], w Writer, r Reader) *Segment {
	return &Segment{
		id:     id,
		status: segment_swapped,
		writer: w,
		reader: r,
	}
}

func (node *Segment) length() int {
	return node.store.Length
}

func (node *Segment) split(hd, tl skiplist.Arc[Addr]) *Segment {
	head := &Segment{
		id:     hd,
		status: node.status,
		store:  node.store,
		writer: node.writer,
		reader: node.reader,
	}

	node.store = head.store.Split(tl.Lo)
	node.id = tl
	return head
}

func (node *Segment) put(key Addr, val string) (bool, error) {
	if node.status == segment_swapped {
		return false, fmt.Errorf("node is swapped")
	}

	isCreated := node.store.Put(key, val)
	if isCreated {
		node.status = segment_dirty
	}

	return isCreated, nil
}

func (node *Segment) values() (skiplist.Iterator[Addr, string], error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return nil, err
		}
	}

	return skiplist.ForMap(node.store, node.store.Keys()), nil
}

func (node *Segment) successors(key Addr) (skiplist.Iterator[Addr, string], error) {
	if node.status == segment_swapped {
		if err := node.read(); err != nil {
			return nil, err
		}
	}

	return skiplist.ForMap(node.store, node.store.Successors(key)), nil
}

func (node *Segment) write() error {
	if node.status != segment_dirty {
		return nil
	}

	if err := node.writer.Write(node.id.Hi, node.store); err != nil {
		return err
	}

	node.status = segment_present

	return nil
}

func (node *Segment) read() error {
	if node.status != segment_swapped {
		return nil
	}

	if node.reader == nil {
		node.store = skiplist.NewMap[Addr, string]()
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
