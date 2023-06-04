//
// Copyright (C) 2023 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/segment
//

package segment

import (
	"github.com/fogfish/guid/v2"
	"github.com/fogfish/skiplist"
)

type Segment[K skiplist.Key, V any] struct {
	ID      guid.K
	Lo, Hi  *skiplist.Pair[K, V]
	Swapped bool
	// write   int
	// read    int
	// scan    int
}

func NewSegment[K skiplist.Key, V any](pair *skiplist.Pair[K, V]) *Segment[K, V] {
	return &Segment[K, V]{
		ID:      guid.G(guid.Clock),
		Lo:      pair,
		Hi:      pair.NextOn(L),
		Swapped: false,
	}
}
