package straw

import (
	"fmt"
	"io"
	"os"
	"sort"
)

var _ StreamStore = &OsStreamStore{}

type OsStreamStore struct {
}

func (_ *OsStreamStore) Lstat(filename string) (os.FileInfo, error) {
	return os.Lstat(filename)
}

func (_ *OsStreamStore) Stat(filename string) (os.FileInfo, error) {
	return os.Stat(filename)
}

func (_ *OsStreamStore) Mkdir(path string, mode os.FileMode) error {
	return os.Mkdir(path, mode)
}

func (_ *OsStreamStore) OpenReadCloser(name string) (io.ReadCloser, error) {
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
	return f, nil
}

func (_ *OsStreamStore) Remove(name string) error {
	return os.Remove(name)
}

func (_ *OsStreamStore) CreateWriteCloser(name string) (io.WriteCloser, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (_ *OsStreamStore) Readdir(name string) ([]os.FileInfo, error) {
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
