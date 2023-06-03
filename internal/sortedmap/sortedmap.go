package sortedmap

import "github.com/fogfish/skiplist"

func Predecessor[K skiplist.Key, V any](
	kv *skiplist.Map[K, V],
	level int,
	key K,
) *skiplist.Pair[K, V] {
	el, path := kv.Skip(level, key)
	if el.Key == key {
		return el
	}

	return path[level]
}

func Cut[K skiplist.Key, V any](
	kv *skiplist.Map[K, V],
	level int,
	node *skiplist.Pair[K, V],
) *skiplist.Pair[K, V] {
	if node == nil {
		return nil
	}

	at := node
	to := at.NextOn(level)
	loSegment := at.Next()

	// sometimes segment is equal to 0
	if loSegment == to {
		return nil
	}

	var hiSegment *skiplist.Pair[K, V]

	if to != nil {
		_, pathToHi := kv.Skip(0, to.Key)
		hiSegment = pathToHi[0]
	}

	for i := 0; i < len(at.Fingers); i++ {
		if at.Fingers[i] != nil && (to == nil || at.Fingers[i].Key < to.Key) {
			at.Fingers[i] = to
		}
	}

	if to != nil {
		// detach last segment from list
		for i := 0; i < len(hiSegment.Fingers); i++ {
			hiSegment.Fingers[i] = nil
		}
	}

	return loSegment
}

// Explicitly create node with given topology
func Push[K skiplist.Key, V any](
	kv *skiplist.Map[K, V],
	seq []K,
	val V,
) *skiplist.Pair[K, V] {
	null := *new(K)
	node := kv.NewPair(seq[0], len(seq)-1)
	node.Key = seq[0]
	node.Value = val

	for i := 1; i < len(seq); i++ {
		if seq[i] != null {
			el, _ := kv.Skip(0, seq[i])
			node.Fingers[i-1] = el
		}
	}

	head := kv.Head()
	for i := 1; i < len(seq); i++ {
		head.Fingers[i-1] = node
	}

	return node
}

func PushH[K skiplist.Key, V any](
	kv *skiplist.Map[K, V],
	seq []K,
) *skiplist.Pair[K, V] {
	null := *new(K)

	head := kv.Head()
	for i := 1; i < len(seq); i++ {
		if seq[i] != null {
			el, _ := kv.Skip(0, seq[i])
			head.Fingers[i-1] = el
		}
	}

	return head
}

func Put[K skiplist.Key, V any](
	kv *skiplist.Map[K, V],
	level int,
	key K,
	val V,
) bool {
	el, path := kv.Skip(0, key)

	if el != nil && el.Key == key {
		return false
	}

	rank, el := kv.CreatePair(level, key, val)

	// re-bind fingers to new node
	for level := 0; level < rank; level++ {
		el.Fingers[level] = path[level].Fingers[level]
		path[level].Fingers[level] = el
	}

	return true
}
