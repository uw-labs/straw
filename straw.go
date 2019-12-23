package straw

import (
	"io"
	"os"
	"syscall"
)

type StrawReader interface {
	io.Reader
	io.Closer
	io.ReaderAt
	SeekStart(offset int64) error
}

type StrawWriter interface {
	io.Writer
	io.Closer
}

type StreamStore interface {
	Close() error
	OpenReadCloser(name string) (StrawReader, error)
	CreateWriteCloser(name string) (StrawWriter, error)
	Lstat(path string) (os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	Readdir(path string) ([]os.FileInfo, error)
	Mkdir(path string, mode os.FileMode) error
	Remove(path string) error
}

func MkdirAll(ss StreamStore, path string, perm os.FileMode) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := ss.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{"mkdir", path, syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent
		err = MkdirAll(ss, path[0:j-1], perm)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = ss.Mkdir(path, perm)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := ss.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}
