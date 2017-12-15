package mgvfs

import (
	"fmt"
	"io"
	"os"
)

var _ Filesystem = &OsFilesystem{}

type OsFilesystem struct {
}

/*
func (osfs *OsFilesystem) OpenFile(filename string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(filename, flag, perm)
}
*/

func (osfs *OsFilesystem) Lstat(filename string) (os.FileInfo, error) {
	return os.Lstat(filename)
}

func (osfs *OsFilesystem) Stat(filename string) (os.FileInfo, error) {
	return os.Stat(filename)
}

func (osfs *OsFilesystem) Mkdir(path string, mode os.FileMode) error {
	return os.Mkdir(path, mode)
}

func (osfs *OsFilesystem) OpenReadCloser(name string) (io.ReadCloser, error) {
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

func (osfs *OsFilesystem) Remove(name string) error {
	return os.Remove(name)
}

func (osfs *OsFilesystem) CreateWriteCloser(name string) (io.WriteCloser, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (osfs *OsFilesystem) Readdir(name string) ([]os.FileInfo, error) {
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
	return fi, nil
}
