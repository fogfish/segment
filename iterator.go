//
// Copyright (C) 2023 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/segment
//

package segment

import (
	"github.com/fogfish/skiplist"
)

type forMap[K skiplist.Key, V any] struct {
	kv       *Map[K, V]
	segments *skiplist.Pair[K, V]
	values   *skiplist.Pair[K, V]
}

func (seq *forMap[K, V]) Key() K {
	return seq.values.Key
}

func (seq *forMap[K, V]) Value() V {
	return seq.values.Value
}

func (seq *forMap[K, V]) Next() bool {
	if seq.values == nil {
		return false
	}

	if seq.segments != nil {
		if seq.values.Next() == seq.segments {
			if err := seq.kv.ensureSegment(seq.segments); err != nil {
				// TODO: How to propagate an error?
				return false
			}
			seq.segments = seq.segments.NextOn(L)
		}
	}

	seq.values = seq.values.Next()
	return seq.values != nil
}
