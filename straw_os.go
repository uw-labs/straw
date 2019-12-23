package straw

import (
	"fmt"
	"io"
	"os"
	"sort"
)

var _ StreamStore = &osStreamStore{}

type osStreamStore struct {
}

func (_ *osStreamStore) Close() error {
	return nil
}

func (_ *osStreamStore) Lstat(filename string) (os.FileInfo, error) {
	return os.Lstat(filename)
}

func (_ *osStreamStore) Stat(filename string) (os.FileInfo, error) {
	return os.Stat(filename)
}

func (_ *osStreamStore) Mkdir(path string, mode os.FileMode) error {
	return os.Mkdir(path, mode)
}

func (_ *osStreamStore) OpenReadCloser(name string) (StrawReader, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.IsDir() {
		f.Close()
		return nil, fmt.Errorf("%s is a directory", name)
	}
	return file{f}, nil
}

type file struct {
	*os.File
}

func (f file) SeekStart(offset int64) error {
	_, err := f.Seek(offset, io.SeekStart)
	return err
}

func (_ *osStreamStore) Remove(name string) error {
	return os.Remove(name)
}

func (_ *osStreamStore) CreateWriteCloser(name string) (StrawWriter, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (_ *osStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	fi, err := f.Readdir(-1)
	if err != nil {
		f.Close()
		return nil, err
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(fi, func(i, j int) bool { return fi[i].Name() < fi[j].Name() })
	return fi, nil
}
