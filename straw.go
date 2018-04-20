package straw

import (
	"io"
	"os"
)

type StrawReader interface {
	io.Reader
	io.Closer
}

type StrawWriter interface {
	io.Writer
	io.Closer
}

type StreamStore interface {
	OpenReadCloser(name string) (StrawReader, error)
	CreateWriteCloser(name string) (StrawWriter, error)
	Lstat(path string) (os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	Readdir(path string) ([]os.FileInfo, error)
	Mkdir(path string, mode os.FileMode) error
	Remove(path string) error
}
