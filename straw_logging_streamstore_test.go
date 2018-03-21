package straw

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
)

var _ StreamStore = &TestLogStreamStore{}

type TestLogStreamStore struct {
	t       *testing.T
	wrapped StreamStore
}

func (fs *TestLogStreamStore) Lstat(name string) (os.FileInfo, error) {
	fs.before("Lstat", name)
	defer fs.after("Lstat", name)
	return fs.wrapped.Lstat(name)
}

func (fs *TestLogStreamStore) Stat(name string) (os.FileInfo, error) {
	fs.before("Stat", name)
	defer fs.after("Stat", name)
	return fs.wrapped.Stat(name)
}

func (fs *TestLogStreamStore) OpenReadCloser(name string) (io.ReadCloser, error) {
	fs.before("Open", name)
	defer fs.after("Open", name)
	return fs.wrapped.OpenReadCloser(name)
}

func (fs *TestLogStreamStore) Mkdir(name string, mode os.FileMode) error {
	fs.before("Mkdir", name, mode)
	defer fs.after("Mkdir", name, mode)
	return fs.wrapped.Mkdir(name, mode)
}

func (fs *TestLogStreamStore) Remove(name string) error {
	fs.before("Remove", name)
	defer fs.after("Remove", name)
	return fs.wrapped.Remove(name)
}

func (fs *TestLogStreamStore) CreateWriteCloser(name string) (io.WriteCloser, error) {
	fs.before("CreateWriteOnly", name)
	defer fs.after("CreateWriteOnly", name)
	return fs.wrapped.CreateWriteCloser(name)
}

func (fs *TestLogStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	fs.before("Readdir", name)
	defer fs.after("Readdir", name)
	return fs.wrapped.Readdir(name)
}

func (fs *TestLogStreamStore) before(funcName string, vals ...interface{}) {
	fs.debug(fmt.Sprintf("before %s : ", funcName), vals)
}

func (fs *TestLogStreamStore) after(funcName string, vals ...interface{}) {
	fs.debug(fmt.Sprintf("after %s : ", funcName), vals)
}

func (fs *TestLogStreamStore) debug(s string, i interface{}) {
	fs.t.Logf("\n\n%s\n%s\n%s\n", s, fs.j(i), fs.j(fs.wrapped))
}

func (fs *TestLogStreamStore) j(i interface{}) string {
	j, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(j)
}
