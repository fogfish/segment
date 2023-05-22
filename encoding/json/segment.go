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

	"github.com/fogfish/skiplist"
)

type segmentID[K skiplist.Num] struct {
	Rank uint32 `json:"r"`
	Lo   K      `json:"l"`
	Hi   K      `json:"h"`
}

func EncodeGF2[K skiplist.Num](gf2 *skiplist.GF2[K], w io.Writer) error {
	if _, err := w.Write([]byte("[\n")); err != nil {
		return err
	}

	seq := skiplist.ForGF2(gf2, gf2.Keys())
	for has := seq != nil; has; {
		addr := seq.Value()
		b, err := json.Marshal(segmentID[K]{Rank: addr.Rank, Lo: addr.Lo, Hi: addr.Hi})
		if err != nil {
			return err
		}

		if _, err := w.Write(b); err != nil {
			return err
		}

		has = seq.Next()
		if has {
			if _, err := w.Write([]byte(",\n")); err != nil {
				return err
			}
		}
	}

	if _, err := w.Write([]byte("\n]")); err != nil {
		return err
	}

	return nil
}

func DecodeGF2[K skiplist.Num](gf2 *skiplist.GF2[K], r io.Reader) error {
	c := json.NewDecoder(r)
	t, err := c.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '[' {
		return fmt.Errorf("invalid JSON array")
	}

	for c.More() {
		var id segmentID[K]
		if err = c.Decode(&id); err != nil {
			return err
		}
		gf2.Put(skiplist.Arc[K]{
			Rank: id.Rank,
			Lo:   id.Lo,
			Hi:   id.Hi,
		})
	}

	t, err = c.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != ']' {
		return fmt.Errorf("invalid JSON array")
	}

	return nil
}

type keyval[K skiplist.Num, V any] struct {
	Key K `json:"k"`
	Val V `json:"v"`
}

func EncodeMap[K skiplist.Num, V any](kv *skiplist.Map[K, V], w io.Writer) error {
	if _, err := w.Write([]byte("[\n")); err != nil {
		return err
	}

	seq := skiplist.ForMap(kv, kv.Keys())
	for has := seq != nil; has; {
		b, err := json.Marshal(keyval[K, V]{Key: seq.Key(), Val: seq.Value()})
		if err != nil {
			return err
		}

		if _, err := w.Write(b); err != nil {
			return err
		}

		has = seq.Next()
		if has {
			if _, err := w.Write([]byte(",\n")); err != nil {
				return err
			}
		}
	}

	if _, err := w.Write([]byte("\n]")); err != nil {
		return err
	}

	return nil

}

func DecodeMap[K skiplist.Num, V any](kv *skiplist.Map[K, V], r io.Reader) error {
	c := json.NewDecoder(r)
	t, err := c.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '[' {
		return fmt.Errorf("invalid JSON array")
	}

	for c.More() {
		var keyval keyval[K, V]
		if err = c.Decode(&keyval); err != nil {
			return err
		}
		kv.Put(keyval.Key, keyval.Val)
	}

	t, err = c.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != ']' {
		return fmt.Errorf("invalid JSON array")
	}

	return nil
}
