package straw_test

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/straw"
)

func TestWalk(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	ss.Mkdir("a", 0755)
	writeFile(ss, "a/1")
	writeFile(ss, "b")
	ss.Mkdir("c", 0755)

	var found []string
	var fiNames []string
	var fiIsDirs []bool

	err := straw.Walk(ss, "/", func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		found = append(found, name)
		fiNames = append(fiNames, fi.Name())
		fiIsDirs = append(fiIsDirs, fi.IsDir())
		return nil
	})
	assert.NoError(err)
	assert.Equal([]string{"/", "/a", "/a/1", "/b", "/c"}, found)
	assert.Equal([]string{"", "a", "1", "b", "c"}, fiNames)
	assert.Equal([]bool{true, true, false, false, true}, fiIsDirs)
}

func TestWalkSkipDir(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	ss.Mkdir("a", 0755)
	writeFile(ss, "a/1")
	writeFile(ss, "b")
	ss.Mkdir("c", 0755)

	var found []string
	var fiNames []string
	var fiIsDirs []bool

	err := straw.Walk(ss, "/", func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if name == "/a" {
			return straw.SkipDir
		}
		found = append(found, name)
		fiNames = append(fiNames, fi.Name())
		fiIsDirs = append(fiIsDirs, fi.IsDir())
		return nil
	})
	assert.NoError(err)
	assert.Equal([]string{"/", "/b", "/c"}, found)
	assert.Equal([]string{"", "b", "c"}, fiNames)
	assert.Equal([]bool{true, false, true}, fiIsDirs)
}

func TestWalkExitOnErr(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	ss.Mkdir("a", 0755)
	writeFile(ss, "a/1")
	writeFile(ss, "b")
	ss.Mkdir("c", 0755)

	var found []string
	var fiNames []string
	var fiIsDirs []bool

	someError := errors.New("some random error")

	err := straw.Walk(ss, "/", func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if name == "/b" {
			return someError
		}
		found = append(found, name)
		fiNames = append(fiNames, fi.Name())
		fiIsDirs = append(fiIsDirs, fi.IsDir())
		return nil
	})
	assert.Equal(err, someError)
	assert.Equal([]string{"/", "/a", "/a/1"}, found)
	assert.Equal([]string{"", "a", "1"}, fiNames)
	assert.Equal([]bool{true, true, false}, fiIsDirs)
}
func TestWalkRootNotExist(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	err := straw.Walk(ss, "/this/doesnt/exist", func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		panic("won't get here")
	})
	assert.Error(err, "file does not exist")
}

func writeFile(ss straw.StreamStore, name string) {
	wc, _ := ss.CreateWriteCloser(name)
	wc.Write([]byte{0})
	wc.Close()
}
