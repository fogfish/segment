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

	"github.com/fogfish/segment"
	"github.com/fogfish/skiplist"
)

type segmentID struct {
	Rank uint32       `json:"r"`
	Lo   segment.Addr `json:"l"`
	Hi   segment.Addr `json:"h"`
}

func EncodeGF2(gf2 *skiplist.GF2[segment.Addr], w io.Writer) error {
	if _, err := w.Write([]byte(`[`)); err != nil {
		return err
	}

	seq := skiplist.ForGF2(gf2, gf2.Keys())
	for has := seq != nil; has; {
		addr := seq.Value()
		b, err := json.Marshal(segmentID{Rank: addr.Rank, Lo: addr.Lo, Hi: addr.Hi})
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

	if _, err := w.Write([]byte(`]`)); err != nil {
		return err
	}

	return nil
}

func DecodeGF2(gf2 *skiplist.GF2[segment.Addr], r io.Reader) error {
	c := json.NewDecoder(r)
	t, err := c.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '[' {
		return fmt.Errorf("invalid JSON array")
	}

	for c.More() {
		var id segmentID
		if err = c.Decode(&id); err != nil {
			return err
		}
		gf2.Put(skiplist.Arc[segment.Addr]{
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

type keyval struct {
	Key segment.Addr `json:"k"`
	Val string       `json:"v"`
}

func EncodeMap(kv *skiplist.Map[segment.Addr, string], w io.Writer) error {
	if _, err := w.Write([]byte(`[`)); err != nil {
		return err
	}

	seq := skiplist.ForMap(kv, kv.Keys())
	for has := seq != nil; has; {
		b, err := json.Marshal(keyval{Key: seq.Key(), Val: seq.Value()})
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

	if _, err := w.Write([]byte(`]`)); err != nil {
		return err
	}

	return nil

}

func DecodeMap(kv *skiplist.Map[segment.Addr, string], r io.Reader) error {
	c := json.NewDecoder(r)
	t, err := c.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '[' {
		return fmt.Errorf("invalid JSON array")
	}

	for c.More() {
		var keyval keyval
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
