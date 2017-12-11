package mgvfs

import (
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

func (osfs *OsFilesystem) Open(name string) (io.ReadCloser, error) {
	return os.Open(name)
}

func (osfs *OsFilesystem) Remove(name string) error {
	return os.Remove(name)
}

func (osfs *OsFilesystem) CreateWriteOnly(name string) (io.WriteCloser, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (osfs *OsFilesystem) Readdir(path string) ([]os.FileInfo, error) {
	panic("write me")
}
