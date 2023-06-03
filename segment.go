package segment

import (
	"github.com/fogfish/guid/v2"
	"github.com/fogfish/skiplist"
)

type Segment[K skiplist.Key, V any] struct {
	ID      guid.K
	Lo, Hi  *skiplist.Pair[K, V]
	Swapped bool
}
