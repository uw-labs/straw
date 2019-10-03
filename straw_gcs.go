package straw

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var _ StreamStore = &GCSStreamStore{}

func NewGCSStreamStore(credentialsFile string, bucket string) (*GCSStreamStore, error) {
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		return nil, err
	}

	ss := &GCSStreamStore{
		client: gcsClient,
		bucket: bucket,
	}

	return ss, nil
}

type GCSStreamStore struct {
	client *storage.Client
	bucket string
}

func (fs *GCSStreamStore) Lstat(name string) (os.FileInfo, error) {
	// GCS does not support symlinks
	return fs.Stat(name)
}

func (fs *GCSStreamStore) Stat(name string) (os.FileInfo, error) {
	name = fs.noSlashPrefix(name)
	name = fs.noSlashSuffix(name)

	if name == "" {
		return &gcsStatResult{
			name:  "/",
			isDir: true,
			size:  4096,
		}, nil
	}

	input := storage.Query{
		Prefix:    name,
		Delimiter: "/",
	}
	iter := fs.client.Bucket(fs.bucket).Objects(context.Background(), &input)

	var matching []os.FileInfo

attrLoop:
	for {
		attrs, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break attrLoop
			}
			return nil, err
		}
		if attrs.Name == name {
			matching = append(matching, &gcsStatResult{
				name:    fs.lastElem(name),
				modTime: attrs.Updated,
				size:    attrs.Size,
			})
		} else if fs.noSlashSuffix(attrs.Prefix) == name {
			matching = append(matching, &gcsStatResult{
				name:  fs.lastElem(name),
				isDir: true,
				// modTime: ??,
				size: 4096,
			})
		}
	}

	switch len(matching) {
	case 0:
		return nil, os.ErrNotExist
	case 1:
		return matching[0], nil
	default:
		panic("bug?")
	}
}

type gcsStatResult struct {
	name    string
	isDir   bool
	modTime time.Time
	size    int64
}

func (sr *gcsStatResult) Name() string {
	return sr.name
}

func (sr *gcsStatResult) IsDir() bool {
	return sr.isDir
}

func (sr *gcsStatResult) Size() int64 {
	return sr.size
}

func (sr *gcsStatResult) ModTime() time.Time {
	return sr.modTime
}

func (sr *gcsStatResult) Mode() os.FileMode {
	if sr.IsDir() {
		return os.ModeDir | 0755
	}
	return 0644
}

func (sr *gcsStatResult) Sys() interface{} {
	return nil
}

func (fs *GCSStreamStore) OpenReadCloser(name string) (StrawReader, error) {
	fi, err := fs.Stat(name)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	nameNoSlash := fs.noSlashPrefix(name)
	r, err := fs.client.Bucket(fs.bucket).Object(nameNoSlash).NewReader(context.Background())
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	return &GCSReader{r, fs, nameNoSlash}, nil
}

type GCSReader struct {
	rc      *storage.Reader
	ss      *GCSStreamStore
	objName string
}

func (r *GCSReader) Read(buf []byte) (int, error) {
	return r.rc.Read(buf)
}

func (r *GCSReader) Close() error {
	return r.rc.Close()
}

func (r *GCSReader) ReadAt(buf []byte, start int64) (int, error) {
	rdr, err := r.ss.client.Bucket(r.ss.bucket).Object(r.objName).NewRangeReader(context.Background(), start, int64(len(buf)))
	if err != nil {
		return 0, err
	}
	i, err := io.ReadFull(rdr, buf)
	if err == io.ErrUnexpectedEOF {
		// Not unexpected.
		err = io.EOF
	}
	return i, err
}

func (fs *GCSStreamStore) Mkdir(name string, mode os.FileMode) error {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}
	name = fs.noSlashPrefix(name)

	if err := fs.checkParentDir(name); err != nil {
		return err
	}

	if _, err := fs.Stat(name); err == nil {
		return fmt.Errorf("%s : file exists", name)
	}

	obj := fs.client.Bucket(fs.bucket).Object(name)
	w := obj.NewWriter(context.Background())

	if _, err := w.Write([]byte{}); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (fs *GCSStreamStore) checkParentDir(child string) error {
	child = fs.noSlashPrefix(child)
	child = fs.noSlashSuffix(child)

	d, _ := filepath.Split(child)
	if d != "" {
		fi, err := fs.Stat(d)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("%s not a directory", d)
		}
	}
	return nil
}

func (fs *GCSStreamStore) Remove(name string) error {
	fi, err := fs.Stat(name)
	if err != nil {
		return err
	}
	name = fs.noSlashPrefix(name)

	if fi.IsDir() {
		files, err := fs.Readdir(name)
		if err != nil {
			return err
		}
		if len(files) != 0 {
			return fmt.Errorf("%s : directory not empty", name)
		}
		name = fs.fixTrailingSlash(name, true)
	}

	return fs.client.Bucket(fs.bucket).Object(name).Delete(context.Background())
}

func (fs *GCSStreamStore) CreateWriteCloser(name string) (StrawWriter, error) {
	name = fs.noSlashPrefix(name)

	if err := fs.checkParentDir(name); err != nil {
		return nil, err
	}

	if fi, err := fs.Stat(name); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	return fs.client.Bucket(fs.bucket).Object(name).NewWriter(context.Background()), nil
}

func (fs *GCSStreamStore) noSlashPrefix(s string) string {
	return strings.TrimPrefix(s, "/")
}

func (fs *GCSStreamStore) noSlashSuffix(s string) string {
	if strings.HasSuffix(s, "/") {
		return s[:len(s)-1]
	}
	return s
}

func (fs *GCSStreamStore) fixTrailingSlash(s string, wantSlash bool) string {
	if !wantSlash {
		return strings.TrimSuffix(s, "/")
	}
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}

func (fs *GCSStreamStore) lastElem(s string) string {
	_, f := filepath.Split(fs.noSlashSuffix(s))
	return f
}

func (fs *GCSStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}
	name = strings.TrimPrefix(name, "/")

	var results []os.FileInfo

	input := storage.Query{
		Prefix:    name,
		Delimiter: "/",
	}
	iter := fs.client.Bucket(fs.bucket).Objects(context.Background(), &input)

attrLoop:
	for {
		attrs, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break attrLoop
			}
			return nil, err
		}
		if attrs.Name != "" {
			if attrs.Name != name {
				result := &gcsStatResult{
					name:    strings.TrimPrefix(attrs.Name, name),
					modTime: attrs.Updated,
					size:    attrs.Size,
				}
				results = append(results, result)
			}
		} else if attrs.Prefix != "" {
			result := &gcsStatResult{
				name:  fs.noSlashSuffix(strings.TrimPrefix(attrs.Prefix, name)), // a bit confusing because prefix is used in different contexts here.
				isDir: true,
				// modTime: ??
				size: 4096,
			}
			results = append(results, result)
		} else {
			panic("bug?")
		}

	}
	sort.Slice(results, func(i, j int) bool { return results[i].Name() < results[j].Name() })
	return results, nil
}
