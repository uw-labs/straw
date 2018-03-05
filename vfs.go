package govfs

import (
	"io"
	"os"
)

type Filesystem interface {
	OpenReadCloser(name string) (io.ReadCloser, error)
	CreateWriteCloser(name string) (io.WriteCloser, error)
	Lstat(path string) (os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	Readdir(path string) ([]os.FileInfo, error)
	Mkdir(path string, mode os.FileMode) error
	Remove(path string) error
}

/*
type File interface {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Writer
	io.WriterAt
}
*/
