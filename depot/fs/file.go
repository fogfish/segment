package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fogfish/segment/encoding/json"
	"github.com/fogfish/skiplist"
)

type File[K skiplist.Num, V any] struct {
	dir string
}

func NewFile[K skiplist.Num, V any](dir string, perm os.FileMode) (*File[K, V], error) {
	if err := os.MkdirAll(dir, perm); err != nil {
		return nil, err
	}

	return &File[K, V]{dir: dir}, nil
}

func (f *File[K, V]) WriteMeta(gf2 *skiplist.GF2[K]) error {
	fd, err := os.Create(filepath.Join(f.dir, "meta.json"))
	if err != nil {
		return err
	}
	defer fd.Close()

	if err := json.EncodeGF2(gf2, fd); err != nil {
		return err
	}

	return nil
}

func (f *File[K, V]) Write(addr K, kv *skiplist.Map[K, V]) error {
	if kv.Length == 0 {
		fmt.Printf("==> skip %x\n", addr)
		return nil
	}

	name := fmt.Sprintf("%08x.json", addr)
	fd, err := os.Create(filepath.Join(f.dir, name))
	if err != nil {
		return err
	}
	defer fd.Close()

	if err := json.EncodeMap(kv, fd); err != nil {
		return err
	}

	return nil
}

func (f *File[K, V]) ReadMeta() (*skiplist.GF2[K], error) {
	fd, err := os.Open(filepath.Join(f.dir, "meta.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return skiplist.NewGF2[K](), nil
		}
		return nil, err
	}
	defer fd.Close()

	gf2 := skiplist.NewGF2[K]()
	if err := json.DecodeGF2(gf2, fd); err != nil {
		return nil, err
	}

	return gf2, nil
}

func (f *File[K, V]) Read(addr K) (*skiplist.Map[K, V], error) {
	name := fmt.Sprintf("%08x.json", addr)
	fd, err := os.Open(filepath.Join(f.dir, name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return skiplist.NewMap[K, V](), nil
		}
		return nil, err
	}
	defer fd.Close()

	kv := skiplist.NewMap[K, V]()
	if err := json.DecodeMap(kv, fd); err != nil {
		return nil, err
	}

	return kv, nil
}
