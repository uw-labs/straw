package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/uw-labs/straw"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var _ straw.StreamStore = &gcsStreamStore{}

func init() {
	straw.Register("gs", func(u *url.URL) (straw.StreamStore, error) {
		creds := u.Query().Get("credentialsfile")
		if creds == "" {
			return nil, fmt.Errorf("gs URLs must provide a `credentialsfile` parameter")
		}
		return newGCSStreamStore(creds, u.Host)
	})
}

func newGCSStreamStore(credentialsFile string, bucket string) (*gcsStreamStore, error) {
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		return nil, err
	}

	ss := &gcsStreamStore{
		client: gcsClient,
		bucket: bucket,
		ctx:    ctx,
	}

	return ss, nil
}

type gcsStreamStore struct {
	client *storage.Client
	bucket string
	ctx    context.Context
}

func (fs *gcsStreamStore) Close() error {
	return fs.client.Close()
}

func (fs *gcsStreamStore) Lstat(name string) (os.FileInfo, error) {
	// GCS does not support symlinks
	return fs.Stat(name)
}

func (fs *gcsStreamStore) Stat(name string) (os.FileInfo, error) {
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
	iter := fs.client.Bucket(fs.bucket).Objects(fs.ctx, &input)

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

func (fs *gcsStreamStore) OpenReadCloser(name string) (straw.StrawReader, error) {
	fi, err := fs.Stat(name)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	nameNoSlash := fs.noSlashPrefix(name)
	r, err := fs.client.Bucket(fs.bucket).Object(nameNoSlash).NewReader(fs.ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	return &gcsReader{r, fs, nameNoSlash, fs.ctx, -1}, nil
}

type gcsReader struct {
	r       io.ReadCloser
	ss      *gcsStreamStore
	objName string
	ctx     context.Context

	// -1 means don't seek
	seek int64
}

func (r *gcsReader) Seek(start int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if start < 0 {
			return 0, errors.New("invalid seek position")
		}
		r.seek = start
		return start, nil
	default:
		return 0, fmt.Errorf("seek %d not currently supported in s3 backend", whence)
	}
}

func (r *gcsReader) Close() error {
	return r.r.Close()
}

func (r *gcsReader) Read(buf []byte) (int, error) {
	if r.seek != -1 {
		// we have a deferred seek to do before we read.
		if err := r.Close(); err != nil {
			return 0, err
		}
		r.r = eofRdr

		rdr, err := r.ss.client.Bucket(r.ss.bucket).Object(r.objName).NewRangeReader(r.ctx, r.seek, -1)
		if err != nil {
			if e, ok := err.(*googleapi.Error); ok {
				if e.Code == 416 {
					return 0, io.EOF
				}
			}
			return 0, err
		}
		r.r = rdr
		r.seek = -1
	}

	return r.r.Read(buf)
}

func (r *gcsReader) ReadAt(buf []byte, start int64) (int, error) {
	rdr, err := r.ss.client.Bucket(r.ss.bucket).Object(r.objName).NewRangeReader(r.ctx, start, int64(len(buf)))
	if err != nil {
		return 0, err
	}
	defer rdr.Close()
	i, err := io.ReadFull(rdr, buf)
	if err == io.ErrUnexpectedEOF {
		// Not unexpected.
		err = io.EOF
	}
	return i, err
}

func (fs *gcsStreamStore) Mkdir(name string, mode os.FileMode) error {
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
	w := obj.NewWriter(fs.ctx)

	if _, err := w.Write([]byte{}); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (fs *gcsStreamStore) checkParentDir(child string) error {
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

func (fs *gcsStreamStore) Remove(name string) error {
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

	return fs.client.Bucket(fs.bucket).Object(name).Delete(fs.ctx)
}

func (fs *gcsStreamStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
	name = fs.noSlashPrefix(name)

	if err := fs.checkParentDir(name); err != nil {
		return nil, err
	}

	if fi, err := fs.Stat(name); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	return fs.client.Bucket(fs.bucket).Object(name).NewWriter(fs.ctx), nil
}

func (fs *gcsStreamStore) noSlashPrefix(s string) string {
	return strings.TrimPrefix(s, "/")
}

func (fs *gcsStreamStore) noSlashSuffix(s string) string {
	if strings.HasSuffix(s, "/") {
		return s[:len(s)-1]
	}
	return s
}

func (fs *gcsStreamStore) fixTrailingSlash(s string, wantSlash bool) string {
	if !wantSlash {
		return strings.TrimSuffix(s, "/")
	}
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}

func (fs *gcsStreamStore) lastElem(s string) string {
	_, f := filepath.Split(fs.noSlashSuffix(s))
	return f
}

func (fs *gcsStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}
	name = strings.TrimPrefix(name, "/")

	var results []os.FileInfo

	input := storage.Query{
		Prefix:    name,
		Delimiter: "/",
	}
	iter := fs.client.Bucket(fs.bucket).Objects(fs.ctx, &input)

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

var (
	eofRdr = &eofReader{}
)

type eofReader struct{}

func (r *eofReader) Read(buf []byte) (int, error) {
	return 0, io.EOF
}

func (r *eofReader) Close() error {
	return nil
}
