package mgvfs

import (
	"io"
	"io/ioutil"
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
	testRoot string
}

func (fst *fsTester) TestOpenReadNotExisting(t *testing.T) {
	assert := assert.New(t)

	f, err := fst.fs.Open("/does/not/exist")
	assert.Error(err)
	assert.Nil(f)
}

func (fst *fsTester) TestCreateNewWriteOnly(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "testCreateNewRW")

	f, err := fst.fs.CreateWriteOnly(name)
	require.NoError(err)
	assert.NotNil(f)
	require.NoError(writeAll(f, []byte{0, 1, 2, 3, 4}))
	require.NoError(f.Close())

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Size(), int64(5))
	assert.Equal(fi.IsDir(), false)
}

func (fst *fsTester) TestMkdir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdir")

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
	assert.Error(fst.fs.Mkdir(name, 0755), "file exists")
}

func (fst *fsTester) TestMkdirOnExistingFile(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestMkdirOnExistingFile")
	require.NoError(fst.fs.Mkdir(name, 0755))

	filename := filepath.Join(name, "testfile")
	f, err := fst.fs.CreateWriteOnly(filename)
	require.NoError(err)
	require.NoError(writeAll(f, []byte{0, 1, 2, 3, 4}))
	require.NoError(f.Close())

	assert.Error(fst.fs.Mkdir(filename, 0755), "file exists")
}

func (fst *fsTester) TestRemoveDir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestRemoveDir")

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	fi, err := fst.fs.Stat(name)
	assert.NoError(err)
	assert.NotNil(fi)

	assert.NoError(fst.fs.Remove(name))

	fi, err = fst.fs.Stat(name)
	assert.Error(err, "no such file or directory")
	assert.Nil(fi)
}

func (fst *fsTester) TestOverwrite(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "testOverwrite")

	w1, err := fst.fs.CreateWriteOnly(name)
	require.NoError(err)
	assert.NotNil(w1)
	assert.NoError(writeAll(w1, []byte{0, 1, 2, 3, 4}))
	assert.NoError(w1.Close())

	r1, err := fst.fs.Open(name)
	require.NoError(err)
	assert.NotNil(r1)
	all, err := ioutil.ReadAll(r1)
	assert.NoError(err)
	assert.Equal([]byte{0, 1, 2, 3, 4}, all)

	w2, err := fst.fs.CreateWriteOnly(name)
	assert.NoError(err)
	assert.NotNil(w2)
	assert.NoError(writeAll(w2, []byte{5, 6, 7}))
	assert.NoError(w2.Close())

	r2, err := fst.fs.Open(name)
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
		&fsTester{"osfs", &OsFilesystem{}, tempDir()},
	}

	for _, tester := range all {
		typ := reflect.TypeOf(tester)
		val := reflect.ValueOf(tester)
		nm := typ.NumMethod()
		for i := 0; i < nm; i++ {
			mName := typ.Method(i).Name
			if strings.HasPrefix(mName, "Test") {
				t.Run(tester.name+"_"+mName, val.Method(i).Interface().(func(*testing.T)))
			}
		}
	}
}
