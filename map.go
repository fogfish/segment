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

// Addr
type Addr = uint8

type Writer interface {
	WriteMeta(*skiplist.GF2[Addr]) error
	Write(Addr, *skiplist.Map[Addr, string]) error
}

type Reader interface {
	ReadMeta() (*skiplist.GF2[Addr], error)
	Read(Addr) (*skiplist.Map[Addr, string], error)
}

// Persistent skiplist data structure
type Map struct {
	gf2      *skiplist.GF2[Addr]
	segments map[Addr]*Segment
	capacity int
	writer   Writer
	reader   Reader
}

func New(capacity int, writer Writer, reader Reader) (*Map, error) {
	var (
		gf2 *skiplist.GF2[Addr]
		err error
	)

	if reader != nil {
		gf2, err = reader.ReadMeta()
		if err != nil {
			return nil, err
		}
	}

	if gf2 == nil {
		gf2 = skiplist.NewGF2[Addr]()
	}

	segments := make(map[Addr]*Segment)
	arcs := skiplist.ForGF2(gf2, gf2.Keys())

	for has := arcs != nil; has; has = arcs.Next() {
		id := arcs.Value()
		segments[id.Hi] = newNode(id, writer, reader)
	}

	return &Map{
		gf2:      gf2,
		segments: segments,
		capacity: capacity,
		writer:   writer,
		reader:   reader,
	}, nil
}

func (kv *Map) segmentForKey(key Addr) (*Segment, error) {
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

func (kv *Map) Put(key Addr, val string) (bool, error) {
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

func (kv *Map) split(key Addr) {
	hd, tl := kv.gf2.Add(key)
	if hd.Lo == hd.Hi && hd.Hi == tl.Lo && tl.Lo == tl.Hi {
		// Not possible to split. Node overloaded
		return
	}

	tail := kv.segments[tl.Hi]
	head := tail.split(hd, tl)
	kv.segments[hd.Hi] = head

	if tail.length() > kv.capacity || head.length() > kv.capacity {
		kv.split(key)
	}
}

func (kv *Map) Values() (seq skiplist.Iterator[Addr, string], err error) {
	seq = skiplist.Join(
		skiplist.ForGF2(kv.gf2, kv.gf2.Keys()),
		func(addr Addr, arc skiplist.Arc[Addr]) (subseq skiplist.Iterator[Addr, string]) {
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

func (kv *Map) Successors(key Addr) (seq skiplist.Iterator[Addr, string], err error) {
	seq = skiplist.Join(
		skiplist.ForGF2(kv.gf2, kv.gf2.Successors(key)),
		func(addr Addr, arc skiplist.Arc[Addr]) (subseq skiplist.Iterator[Addr, string]) {
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

func (kv *Map) Sync() error {
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
