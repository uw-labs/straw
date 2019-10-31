package straw

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var _ StreamStore = &memStreamStore{}

func init() {
	Register("mem", func(u *url.URL) (StreamStore, error) {
		return newMemStreamStore(), nil
	})
}

func newMemStreamStore() *memStreamStore {
	return &memStreamStore{Root: &memFile{
		IsDir_: true,
	}}
}

type memStreamStore struct {
	lk   sync.Mutex
	Root *memFile
}

type memFile struct {
	Name_   string
	Content []byte
	IsDir_  bool
	Entries map[string]*memFile
	Modtime time.Time
}

func (mf *memFile) IsDir() bool {
	return mf.IsDir_
}

func (mf *memFile) ModTime() time.Time {
	return mf.Modtime
}

func (mf *memFile) Mode() os.FileMode {
	if mf.IsDir_ {
		return os.FileMode(0755) | os.ModeDir
	}
	return os.FileMode(0644)
}

func (mf *memFile) Name() string {
	return mf.Name_
}

func (mf *memFile) Size() int64 {
	if mf.IsDir_ {
		return 4096
	}
	return int64(len(mf.Content))
}

func (mf *memFile) Sys() interface{} {
	return nil
}

func (fs *memStreamStore) Close() error {
	return nil
}

func (fs *memStreamStore) Lstat(name string) (os.FileInfo, error) {
	return fs.Stat(name)
}

func (fs *memStreamStore) Stat(name string) (os.FileInfo, error) {
	return fs.getExisting(name)
}

func (fs *memStreamStore) OpenReadCloser(name string) (StrawReader, error) {
	fs.lk.Lock()
	defer fs.lk.Unlock()

	file, err := fs.getExistingFile(name)
	if err != nil {
		return nil, err
	}
	return newMemFileReader(file)
}

func newMemFileReader(mf *memFile) (*memFileReader, error) {
	return &memFileReader{bytes.NewReader(mf.Content)}, nil
}

type memFileReader struct {
	*bytes.Reader
}

func (mfr *memFileReader) Close() error {
	return nil
}

func (fs *memStreamStore) Mkdir(name string, mode os.FileMode) error {
	fs.lk.Lock()
	defer fs.lk.Unlock()

	list := fs.Split(name)
	dir := fs.Root
	for _, elem := range list[0 : len(list)-1] {
		dir = dir.Entries[elem]
		if dir == nil {
			return os.ErrNotExist
		}
	}
	newdir := list[len(list)-1]
	if dir.Entries == nil {
		dir.Entries = make(map[string]*memFile)
	} else if dir.Entries[newdir] != nil {
		return errors.New("file exists")
	}
	dir.Entries[newdir] = &memFile{IsDir_: true, Name_: newdir}
	return nil
}

func (fs *memStreamStore) Remove(name string) error {
	fs.lk.Lock()
	defer fs.lk.Unlock()

	list := fs.Split(name)
	parent := fs.Root
	for _, elem := range list[0 : len(list)-1] {
		parent = parent.Entries[elem]
		if parent == nil {
			return os.ErrNotExist
		}
	}
	filename := list[len(list)-1]
	if parent.Entries == nil {
		return os.ErrNotExist
	}
	if parent.Entries[filename] == nil {
		return os.ErrNotExist
	}
	file := parent.Entries[filename]
	if file == nil {
		return os.ErrNotExist
	}
	if file.IsDir_ && file.Entries != nil && len(file.Entries) != 0 {
		return errors.New("directory not empty")
	}
	delete(parent.Entries, filename)
	return nil
}

func (fs *memStreamStore) getExistingFile(name string) (*memFile, error) {
	file, err := fs.getExisting(name)
	if err != nil {
		return nil, err
	}
	if file.IsDir_ {
		return nil, fmt.Errorf("%s is a directory", name)
	}
	return file, nil
}

func (fs *memStreamStore) getExisting(name string) (*memFile, error) {
	list := fs.Split(name)
	f := fs.Root
	for _, elem := range list {
		f = f.Entries[elem]
		if f == nil {
			break
		}
	}
	if f == nil {
		return nil, os.ErrNotExist
	}
	return f, nil
}

func (fs *memStreamStore) CreateWriteCloser(name string) (StrawWriter, error) {
	fs.lk.Lock()
	defer fs.lk.Unlock()

	list := fs.Split(name)
	dir := fs.Root
	for _, elem := range list[0 : len(list)-1] {
		dir = dir.Entries[elem]
		if dir == nil {
			return nil, errors.New("not found")
		}
	}
	if !dir.IsDir() {
		return nil, errors.New("not a directory")
	}

	fileName := list[len(list)-1]

	f := dir.Entries[fileName]
	if f == nil {
		f = &memFile{Name_: fileName}
		if dir.Entries == nil {
			dir.Entries = make(map[string]*memFile)
		}
		dir.Entries[fileName] = f
	}
	if f.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}
	f.Content = f.Content[0:0]
	return &memfileWriteCloser{f}, nil
}

type memfileWriteCloser struct {
	mf *memFile
}

func (mfwc *memfileWriteCloser) Write(buf []byte) (int, error) {
	mfwc.mf.Content = append(mfwc.mf.Content, buf...)
	return len(buf), nil
}

func (mfwc *memfileWriteCloser) Close() error {
	return nil
}

func (fs *memStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	file, err := fs.getExisting(name)
	if err != nil {
		return nil, err
	}
	if !file.IsDir() {
		return nil, fmt.Errorf("%v is not a dir", name)
	}
	var res []os.FileInfo
	for _, entry := range file.Entries {
		res = append(res, entry)
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Name() < res[j].Name() })
	return res, nil
}

func (fs *memStreamStore) Split(name string) []string {
	if name == "" {
		return []string{}
	}
	spl := strings.Split(name, string(os.PathSeparator))
	if spl[0] == "" {
		spl = spl[1:]
	}
	if spl[len(spl)-1] == "" {
		spl = spl[0 : len(spl)-1]
	}
	return spl
}
