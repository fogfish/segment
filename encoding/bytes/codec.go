//
// Copyright (C) 2023 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/segment
//

package bytes

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/fogfish/guid/v2"
)

type WriterTyped struct {
	w    io.Writer
	Fail error
}

func NewWriterTyped(w io.Writer) *WriterTyped { return &WriterTyped{w: w} }

type ReaderTyped struct {
	r    io.Reader
	Fail error
}

func NewReaderTyped(r io.Reader) *ReaderTyped { return &ReaderTyped{r: r} }

func (w *WriterTyped) Write(p []byte) (int, error) {
	if w.Fail != nil {
		return 0, w.Fail
	}

	return w.w.Write(p)
}

func (r *ReaderTyped) Read(p []byte) (int, error) {
	if r.Fail != nil {
		return 0, r.Fail
	}

	return r.r.Read(p)
}

// -------------------------------------------------------------------------

func (w *WriterTyped) WriteUInt32(x uint32) error {
	if w.Fail != nil {
		return w.Fail
	}

	var b []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(b, x)
	_, w.Fail = w.Write(b)

	return w.Fail
}

func (r *ReaderTyped) ReadUInt32(x *uint32) error {
	if r.Fail != nil {
		return r.Fail
	}

	var b []byte = make([]byte, 4)
	if _, r.Fail = io.ReadFull(r, b); r.Fail != nil {
		return r.Fail
	}

	*x = binary.BigEndian.Uint32(b)
	return r.Fail
}

// -------------------------------------------------------------------------

func (w *WriterTyped) WriteString(x string) error {
	if w.Fail != nil {
		return w.Fail
	}

	if err := w.WriteUInt32(uint32(len(x))); err != nil {
		return err
	}

	_, w.Fail = w.Write([]byte(x))
	return w.Fail
}

func (r *ReaderTyped) ReadString(x *string) error {
	if r.Fail != nil {
		return r.Fail
	}

	var size uint32
	if err := r.ReadUInt32(&size); err != nil {
		return err
	}

	bytes := make([]byte, size)
	if _, r.Fail = io.ReadFull(r, bytes); r.Fail != nil {
		return r.Fail
	}

	*x = string(bytes)
	return r.Fail
}

// -------------------------------------------------------------------------

func (w *WriterTyped) WriteGUID(x guid.K) error {
	if w.Fail != nil {
		return w.Fail
	}

	// 12 bytes
	_, w.Fail = w.Write(guid.Bytes(x))
	return w.Fail
}

func (r *ReaderTyped) ReadGUID(x *guid.K) error {
	if r.Fail != nil {
		return r.Fail
	}

	// 12 bytes
	var b []byte = make([]byte, 12)
	if _, r.Fail = io.ReadFull(r, b); r.Fail != nil {
		return r.Fail
	}

	*x, r.Fail = guid.FromBytes(b)
	return r.Fail
}

// -------------------------------------------------------------------------

// TODO: deal with nil

func (w *WriterTyped) WriteValue(x any) error {
	if w.Fail != nil {
		return w.Fail
	}

	switch v := x.(type) {
	case uint32:
		return w.WriteUInt32(v)
	case string:
		return w.WriteString(v)
		// case interface{ EncodeBytes(io.Writer) error }:
	// 	if err := v.EncodeBytes(w); err != nil {
	// 		return err
	// 	}
	default:
		return fmt.Errorf("encoder does not support type %T", x)
	}
}

func (r *ReaderTyped) ReadValue(x any) error {
	if r.Fail != nil {
		return r.Fail
	}

	switch v := x.(type) {
	case *uint32:
		return r.ReadUInt32(v)
	case *string:
		return r.ReadString(v)
		// case interface{ EncodeBytes(io.Writer) error }:
	// 	if err := v.EncodeBytes(w); err != nil {
	// 		return err
	// 	}
	default:
		return fmt.Errorf("encoder does not support type %T", x)
	}

}
