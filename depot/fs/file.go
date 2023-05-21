package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fogfish/segment"
	"github.com/fogfish/segment/encoding/json"
	"github.com/fogfish/skiplist"
)

type File struct {
	dir string
}

func NewFile(dir string, perm os.FileMode) (*File, error) {
	if err := os.MkdirAll(dir, perm); err != nil {
		return nil, err
	}

	return &File{dir: dir}, nil
}

func (f *File) WriteMeta(gf2 *skiplist.GF2[segment.Addr]) error {
	fd, err := os.Create(filepath.Join(f.dir, "meta.json"))
	if err != nil {
		return err
	}

	if err := json.EncodeGF2(gf2, fd); err != nil {
		return err
	}

	return nil
}

func (f *File) Write(addr segment.Addr, kv *skiplist.Map[segment.Addr, string]) error {
	if kv.Length == 0 {
		return nil
	}

	name := fmt.Sprintf("%08x.json", addr)
	fd, err := os.Create(filepath.Join(f.dir, name))
	if err != nil {
		return err
	}

	if err := json.EncodeMap(kv, fd); err != nil {
		return err
	}

	return nil
}

func (f *File) ReadMeta() (*skiplist.GF2[segment.Addr], error) {
	fd, err := os.Open(filepath.Join(f.dir, "meta.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return skiplist.NewGF2[segment.Addr](), nil
		}
		return nil, err
	}

	gf2 := skiplist.NewGF2[segment.Addr]()
	if err := json.DecodeGF2(gf2, fd); err != nil {
		return nil, err
	}

	return gf2, nil
}

func (f *File) Read(addr segment.Addr) (*skiplist.Map[segment.Addr, string], error) {
	name := fmt.Sprintf("%08x.json", addr)
	fd, err := os.Open(filepath.Join(f.dir, name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return skiplist.NewMap[segment.Addr, string](), nil
		}
		return nil, err
	}

	kv := skiplist.NewMap[segment.Addr, string]()
	if err := json.DecodeMap(kv, fd); err != nil {
		return nil, err
	}

	return kv, nil
}
