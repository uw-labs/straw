package mgvfs

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fsTester struct {
	name     string
	fs       Filesystem
	ff       func() Filesystem
	testRoot string
}

func (fst *fsTester) TestOpenReadNotExisting(t *testing.T) {
	assert := assert.New(t)

	f, err := fst.fs.OpenReadCloser("/does/not/exist")
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
	assert.Nil(f)
}

func (fst *fsTester) TestCreateNewWriteOnly(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestCreateNewWriteOnly")

	f, err := fst.fs.CreateWriteCloser(name)
	require.NoError(err)
	assert.NotNil(f)
	require.NoError(writeAll(f, []byte{0, 1, 2, 3, 4}))
	require.NoError(f.Close())

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Size(), int64(5))
	assert.Equal(fi.IsDir(), false)

	files, err := fst.fs.Readdir(fst.testRoot)
	require.NoError(err)
	assert.Equal(1, len(files))

	assert.False(files[0].IsDir())
	assert.Equal("TestCreateNewWriteOnly", files[0].Name())
	assert.Equal(int64(5), files[0].Size())
	assert.Equal(os.FileMode(0644), files[0].Mode())
}

func (fst *fsTester) TestMkdirAtRoot(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdirAtRoot")

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Size(), int64(4096))
	assert.Equal(fi.IsDir(), true)
}

func (fst *fsTester) TestMkdirTrailingSlash(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdirTrailingSlash")
	name = name + "/"

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Size(), int64(4096))
	assert.Equal(fi.IsDir(), true)
}

func (fst *fsTester) TestMkdirOnExistingDir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdirOnExistingDir")

	require.NoError(fst.fs.Mkdir(name, 0755))

	err := fst.fs.Mkdir(name, 0755)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "file exists") })
}

func (fst *fsTester) TestMkdirOnExistingFile(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdirOnExistingFile")
	require.NoError(fst.fs.Mkdir(name, 0755))

	filename := filepath.Join(name, "testfile")
	f, err := fst.fs.CreateWriteCloser(filename)
	require.NoError(err)
	require.NoError(writeAll(f, []byte{0, 1, 2, 3, 4}))
	require.NoError(f.Close())

	err = fst.fs.Mkdir(filename, 0755)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "file exists") })
}

func (fst *fsTester) TestMkdirInNonExistingDir(t *testing.T) {
	assert := assert.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdirInNonExistingDir")
	name = filepath.Join(name, "innerdir")
	err := fst.fs.Mkdir(name, 0755)

	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
}

func (fst *fsTester) TestRemoveNonExistingAtRoot(t *testing.T) {
	assert := assert.New(t)

	err := fst.fs.Remove("not_existing_file")
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
}

func (fst *fsTester) TestRemoveNonExistingInSubdir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	top := filepath.Join(fst.testRoot, "TestRemoveNonExistingInSubdir")
	require.NoError(fst.fs.Mkdir(top, 0755))

	err := fst.fs.Remove(filepath.Join(top, "not_existing_file"))
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
}

func (fst *fsTester) TestRemoveParentDirDoesNotExist(t *testing.T) {
	assert := assert.New(t)

	parent := filepath.Join(fst.testRoot, "TestRemoveParentDirDoesNotExist")
	child := filepath.Join(parent, "some_filename")

	err := fst.fs.Remove(child)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
}

func (fst *fsTester) TestRemoveEmptyDir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestRemoveEmptyDir")

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	fi, err := fst.fs.Stat(name)
	assert.NoError(err)
	assert.NotNil(fi)

	assert.NoError(fst.fs.Remove(name))

	fi, err = fst.fs.Stat(name)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
	assert.Nil(fi)
}

func (fst *fsTester) TestRemoveNonEmptyDir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestRemoveNonEmptyDir")

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	w1, err := fst.fs.CreateWriteCloser(filepath.Join(name, "a_file"))
	require.NoError(err)
	assert.NoError(writeAll(w1, []byte{0, 1, 2, 3, 4}))
	assert.NoError(w1.Close())

	err = fst.fs.Remove(name)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "directory not empty") })

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Name(), "TestRemoveNonEmptyDir")
}

func (fst *fsTester) TestOverwrite(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestOverwrite")

	w1, err := fst.fs.CreateWriteCloser(name)
	require.NoError(err)
	assert.NotNil(w1)
	assert.NoError(writeAll(w1, []byte{0, 1, 2, 3, 4}))

	assert.NoError(w1.Close())

	r1, err := fst.fs.OpenReadCloser(name)
	require.NoError(err)
	assert.NotNil(r1)
	all, err := ioutil.ReadAll(r1)
	assert.NoError(err)
	assert.Equal([]byte{0, 1, 2, 3, 4}, all)

	w2, err := fst.fs.CreateWriteCloser(name)
	assert.NoError(err)
	assert.NotNil(w2)
	assert.NoError(writeAll(w2, []byte{5, 6, 7}))
	assert.NoError(w2.Close())

	r2, err := fst.fs.OpenReadCloser(name)
	assert.NoError(err)
	assert.NotNil(r2)
	all, err = ioutil.ReadAll(r2)
	assert.NoError(err)
	assert.Equal([]byte{5, 6, 7}, all)
}

/*

func (fst *fsTester) TestAppend(t *testing.T) {
	assert := assert.New(t)

	name := filepath.Join(tempdir, "testAppend")

	f, err := fst.fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	assert.NoError(err)
	assert.NotNil(f)
	assert.NoError(writeAll(f, []byte{0, 1, 2, 3, 4}))
	assert.NoError(f.Close())

	f, err = fst.fs.OpenFile(name, os.O_RDONLY, 0)
	assert.NoError(err)
	assert.NotNil(f)
	all, err := ioutil.ReadAll(f)
	assert.NoError(err)
	assert.Equal([]byte{0, 1, 2, 3, 4}, all)

	f, err = fst.fs.OpenFile(name, os.O_RDWR|os.O_APPEND, 0666)
	assert.NoError(err)
	assert.NotNil(f)
	assert.NoError(writeAll(f, []byte{5, 6, 7}))
	assert.NoError(f.Close())

	f, err = fst.fs.OpenFile(name, os.O_RDONLY, 0)
	assert.NoError(err)
	assert.NotNil(f)
	all, err = ioutil.ReadAll(f)
	assert.NoError(err)
	assert.Equal([]byte{0, 1, 2, 3, 4, 5, 6, 7}, all)

}

func (fst *fsTester) TestWriteAtCreate(t *testing.T) {
	assert := assert.New(t)

	name := filepath.Join(tempdir, "testWriteAtCreate")
	f, err := fst.fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	assert.NoError(err)
	assert.NotNil(f)

	i, err := f.WriteAt([]byte{1, 2}, 14)
	assert.NoError(err)
	assert.Equal(2, i)
	assert.NoError(f.Close())

	fi, err := fst.fs.Stat(name)
	assert.NoError(err)
	assert.Equal(fi.Size(), int64(16))

	f, err = fst.fs.OpenFile(name, os.O_RDONLY, 0)
	assert.NoError(err)
	assert.NotNil(f)
	all, err := ioutil.ReadAll(f)
	assert.NoError(err)
	assert.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2}, all)
}
*/

func writeAll(w io.Writer, data []byte) error {
	i, err := w.Write(data)
	if err != nil {
		return err
	}
	if i != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func tempDir() string {
	tempdir, err := ioutil.TempDir("", "mgvfs_test_")
	if err != nil {
		panic(err)
	}
	return tempdir
}

func TestAll(t *testing.T) {

	all := []*fsTester{
		&fsTester{"osfs", nil, func() Filesystem { return &TestLogFilesystem{t, &OsFilesystem{}} }, tempDir()},
		&fsTester{"memfs", nil, func() Filesystem { return &TestLogFilesystem{t, NewMemFilesystem()} }, "/"},
	}

	for _, tester := range all {
		typ := reflect.TypeOf(tester)
		val := reflect.ValueOf(tester)
		nm := typ.NumMethod()
		for i := 0; i < nm; i++ {
			mName := typ.Method(i).Name
			if strings.HasPrefix(mName, "Test") {
				tester.fs = tester.ff()
				t.Run(tester.name+"_"+mName, val.Method(i).Interface().(func(*testing.T)))
			}
		}
	}
}
