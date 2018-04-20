package straw

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
	fs       StreamStore
	ff       func() StreamStore
	testRoot string
}

func (fst *fsTester) TestOpenReadCloserNotExisting(t *testing.T) {
	assert := assert.New(t)

	f, err := fst.fs.OpenReadCloser("/does/not/exist")
	assert.True(os.IsNotExist(err))
	assert.Nil(f)
}

func (fst *fsTester) TestOpenReadCloserOnDirectory(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestOpenReadCloserOnDirectory")

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	f, err := fst.fs.OpenReadCloser(name)
	assert.EqualError(err, fmt.Sprintf("%s is a directory", name))
	//	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "no such file or directory") })
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

func (fst *fsTester) TestCreateWriteOnlyOnExistingDir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	name := filepath.Join(fst.testRoot, "TestCreateWriteOnlyOnExistingDir")

	err := fst.fs.Mkdir(name, 0755)
	require.NoError(err)

	f, err := fst.fs.CreateWriteCloser(name)
	require.NotNil(err)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "is a directory") })
	assert.Nil(f)

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Size(), int64(4096))
	assert.Equal(fi.IsDir(), true)
}

func (fst *fsTester) TestCreateWriteOnlyInExistingFile(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	filename := filepath.Join(fst.testRoot, "TestCreateWriteOnlyInExistingFile")
	f, err := fst.fs.CreateWriteCloser(filename)
	require.NoError(err)
	require.NoError(writeAll(f, []byte{0, 1, 2, 3, 4}))
	require.NoError(f.Close())

	name := filepath.Join(filename, "another_filename")

	f, err = fst.fs.CreateWriteCloser(name)
	require.NotNil(err)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "not a directory") }, "error does not match : %s", err.Error())
	assert.Nil(f)

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
	require.NotNil(err)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "file exists") }, "error does not match: %s", err.Error())
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

	assert.True(os.IsNotExist(err))
}

func (fst *fsTester) TestRemoveNonExistingAtRoot(t *testing.T) {
	assert := assert.New(t)

	err := fst.fs.Remove("not_existing_file")
	assert.True(os.IsNotExist(err))
}

func (fst *fsTester) TestRemoveNonExistingInSubdir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	top := filepath.Join(fst.testRoot, "TestRemoveNonExistingInSubdir")
	require.NoError(fst.fs.Mkdir(top, 0755))

	err := fst.fs.Remove(filepath.Join(top, "not_existing_file"))
	assert.True(os.IsNotExist(err))
}

func (fst *fsTester) TestRemoveParentDirDoesNotExist(t *testing.T) {
	assert := assert.New(t)

	parent := filepath.Join(fst.testRoot, "TestRemoveParentDirDoesNotExist")
	child := filepath.Join(parent, "some_filename")

	err := fst.fs.Remove(child)
	assert.True(os.IsNotExist(err))
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
	assert.Nil(fi)
	require.NotNil(err)
	assert.True(os.IsNotExist(err))
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
	require.NotNil(err)
	assert.Condition(func() bool { return strings.HasSuffix(err.Error(), "directory not empty") }, "error does not match : %s", err.Error())

	fi, err := fst.fs.Stat(name)
	require.NoError(err)
	assert.Equal(fi.Name(), "TestRemoveNonEmptyDir")
}

func (fst *fsTester) TestRemoveFileInDir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	dirname := filepath.Join(fst.testRoot, "TestRemoveFileInDir")
	filename := filepath.Join(dirname, "a_file")

	require.NoError(fst.fs.Mkdir(dirname, 0755))
	require.NoError(fst.writeFile(fst.fs, filename, []byte{1}))

	fi, err := fst.fs.Stat(filename)
	assert.NoError(err)
	assert.NotNil(fi)

	assert.NoError(fst.fs.Remove(filename))

	fi, err = fst.fs.Stat(filename)
	assert.Nil(fi)
	require.NotNil(err)
	assert.True(os.IsNotExist(err))
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

func (fst *fsTester) TestReaddir(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	dir := filepath.Join(fst.testRoot, "TestReaddir")
	dir1 := filepath.Join(dir, "dir1")
	file1 := filepath.Join(dir, "file1")
	file2 := filepath.Join(dir1, "file2")

	require.NoError(fst.fs.Mkdir(dir, 0755))
	require.NoError(fst.fs.Mkdir(dir1, 0755))
	require.NoError(fst.writeFile(fst.fs, file1, []byte{1}))
	require.NoError(fst.writeFile(fst.fs, file2, []byte{2}))

	rd1, err := fst.fs.Readdir(dir)
	assert.NoError(err)
	assert.Equal(2, len(rd1))

	assert.Equal("dir1", rd1[0].Name())
	assert.Equal("file1", rd1[1].Name())

	rd2, err := fst.fs.Readdir(dir1)
	assert.NoError(err)
	assert.Equal(1, len(rd2))

	assert.Equal("file2", rd2[0].Name())
}

func (fst *fsTester) TestReaddirMoreThanMaxKeysFiles(t *testing.T) {
	// max keys defaults to 1000
	assert := assert.New(t)
	require := require.New(t)

	dir := filepath.Join(fst.testRoot, "TestReaddirManyFiles")
	require.NoError(fst.fs.Mkdir(dir, 0755))
	for i := 0; i < 1010; i++ {
		if i%100 == 0 {
			log.Printf("created %d files", i)
		}
		file := filepath.Join(dir, fmt.Sprintf("file%d", i))
		require.NoError(fst.writeFile(fst.fs, file, []byte{1}))
	}
	rd1, err := fst.fs.Readdir(dir)
	assert.NoError(err)
	require.Equal(1010, len(rd1))
}

func (fst *fsTester) TestStat(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	dir := filepath.Join(fst.testRoot, "TestStat")
	dir1 := filepath.Join(dir, "dir")
	file := filepath.Join(dir1, "file")

	require.NoError(fst.fs.Mkdir(dir, 0755))
	require.NoError(fst.fs.Mkdir(dir1, 0755))
	require.NoError(fst.writeFile(fst.fs, file, []byte{2}))

	fi, err := fst.fs.Stat(dir1)
	assert.NoError(err)
	assert.Equal(true, fi.IsDir())
	assert.Equal("dir", fi.Name())
	assert.Equal(os.FileMode(0755)|os.ModeDir, fi.Mode())
	assert.Equal(int64(4096), fi.Size())

	fi, err = fst.fs.Stat(file)
	assert.NoError(err)
	assert.Equal(false, fi.IsDir())
	assert.Equal("file", fi.Name())
	assert.Equal(os.FileMode(0644), fi.Mode())
	assert.Equal(int64(1), fi.Size())

	root := "/"
	fi, err = fst.fs.Stat(root)
	assert.NoError(err)
	assert.Equal(true, fi.IsDir())
}

func (fst *fsTester) writeFile(fs StreamStore, name string, data []byte) error {
	w, err := fs.CreateWriteCloser(name)
	if err != nil {
		return err
	}
	if err := writeAll(w, data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
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
	tempdir, err := ioutil.TempDir("", "straw_test_")
	if err != nil {
		panic(err)
	}
	return tempdir
}

func TestOSFS(t *testing.T) {
	testFS(t, "osfs", func() StreamStore { return &TestLogStreamStore{t, &OsStreamStore{}} }, tempDir())
}

func TestMemFS(t *testing.T) {
	testFS(t, "memfs", func() StreamStore { return &TestLogStreamStore{t, NewMemStreamStore()} }, "/")
}

func TestS3FS(t *testing.T) {
	testBucket := os.Getenv("S3_TEST_BUCKET")
	if testBucket == "" {
		t.Skip("S3_TEST_BUCKET not set, skipping tests for s3 backend")
	}

	s3fs, err := NewS3StreamStore(testBucket)
	if err != nil {
		t.Fatal(err)
	}
	testFS(t, "s3fs", func() StreamStore { return &TestLogStreamStore{t, s3fs} }, "/")
}

func testFS(t *testing.T, name string, fsProvider func() StreamStore, rootDir string) {

	tester := &fsTester{name, nil, fsProvider, rootDir}

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

func TestMkdirAll(t *testing.T) {
	assert := assert.New(t)

	ss := NewMemStreamStore()

	assert.NoError(MkdirAll(ss, "/foo/bar/baz/qux/quux/", 0644))

	fis, err := ss.Readdir("/foo/bar/baz/qux/")
	assert.NoError(err)

	assert.Equal(1, len(fis))
	assert.Equal("quux", fis[0].Name())
}

func TestMkdirAllExistingNoError(t *testing.T) {
	assert := assert.New(t)

	ss := NewMemStreamStore()

	assert.NoError(MkdirAll(ss, "/foo/bar/baz/qux/quux/", 0644))
	assert.NoError(MkdirAll(ss, "/foo/bar/baz/qux/quux/", 0644))

}
