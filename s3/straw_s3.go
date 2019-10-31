package s3

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/uw-labs/straw"
)

var _ straw.StreamStore = &s3StreamStore{}

func init() {
	straw.Register("s3", func(u *url.URL) (straw.StreamStore, error) {
		return news3StreamStore(u.Host, u.Query().Get("sse"))
	})
}

func news3StreamStore(bucket string, sseType string) (*s3StreamStore, error) {
	sess, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)

	ss := &s3StreamStore{
		sess:    sess,
		s3:      svc,
		bucket:  bucket,
		sseType: sseType,
	}

	return ss, nil
}

type s3StreamStore struct {
	sess    *session.Session
	s3      *s3.S3
	bucket  string
	sseType string
}

func (fs *s3StreamStore) Close() error {
	// nothing to close for s3 it seems
	return nil
}

func (fs *s3StreamStore) Lstat(name string) (os.FileInfo, error) {
	// S3 does not support symlinks
	return fs.Stat(name)
}

func (fs *s3StreamStore) Stat(name string) (os.FileInfo, error) {
	name = fs.noSlashPrefix(name)
	name = fs.noSlashSuffix(name)

	if name == "" {
		return &s3StatResult{
			name:  "/",
			isDir: true,
			size:  4096,
		}, nil
	}

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		MaxKeys:   aws.Int64(2),
		Prefix:    aws.String(name),
		Delimiter: aws.String("/"),
	}
	out, err := fs.s3.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	var matching []os.FileInfo

	for _, cont := range out.Contents {
		if *cont.Key == name {
			matching = append(matching, &s3StatResult{
				name:    fs.lastElem(*cont.Key),
				modTime: *cont.LastModified,
				size:    *cont.Size,
			})
		}
	}

	for _, prefix := range out.CommonPrefixes {
		if *prefix.Prefix == name+"/" {
			matching = append(matching, &s3StatResult{
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
	case 2:
		panic("bug?")
	default:
		return matching[0], nil
	}
}

type s3StatResult struct {
	name    string
	isDir   bool
	modTime time.Time
	size    int64
}

func (sr *s3StatResult) Name() string {
	return sr.name
}

func (sr *s3StatResult) IsDir() bool {
	return sr.isDir
}

func (sr *s3StatResult) Size() int64 {
	return sr.size
}

func (sr *s3StatResult) ModTime() time.Time {
	return sr.modTime
}

func (sr *s3StatResult) Mode() os.FileMode {
	if sr.IsDir() {
		return os.ModeDir | 0755
	}
	return 0644
}

func (sr *s3StatResult) Sys() interface{} {
	return nil
}

func (fs *s3StreamStore) OpenReadCloser(name string) (straw.StrawReader, error) {
	fi, err := fs.Stat(name)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	input := s3.GetObjectInput{
		Bucket: &fs.bucket,
		Key:    aws.String(name),
	}

	out, err := fs.s3.GetObject(&input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == s3.ErrCodeNoSuchKey {
				return nil, os.ErrNotExist
			}
		}
		return nil, err
	}
	return &s3Reader{out.Body, fs.s3, input}, nil
}

type s3Reader struct {
	rc io.ReadCloser

	s3    *s3.S3
	input s3.GetObjectInput
}

func (r *s3Reader) Read(buf []byte) (int, error) {
	return r.rc.Read(buf)
}

func (r *s3Reader) Close() error {
	return r.rc.Close()
}

func (r *s3Reader) ReadAt(buf []byte, start int64) (int, error) {
	end := int64(len(buf)) + start - 1
	r.input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, end))
	out, err := r.s3.GetObject(&r.input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == s3.ErrCodeNoSuchKey {
				return 0, os.ErrNotExist
			}
		}
		return 0, err
	}
	all, err := ioutil.ReadAll(out.Body)
	if err != nil {
		_ = out.Body.Close()
		return 0, err
	}

	copy(buf, all)

	err = out.Body.Close()

	switch {
	case len(all) == len(buf):
		return len(all), err
	case len(all) < len(buf):
		return len(all), io.EOF
	default:
		panic(fmt.Sprintf("only expected up to %d bytes but got %d", len(buf), len(all)))
	}
}

func (fs *s3StreamStore) Mkdir(name string, mode os.FileMode) error {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}

	if err := fs.checkParentDir(name); err != nil {
		return err
	}

	if _, err := fs.Stat(name); err == nil {
		return fmt.Errorf("%s : file exists", name)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(fs.bucket),
		Key:         aws.String(name),
		ContentType: aws.String("application/x-directory"),
	}

	if fs.sseType != "" {
		input.ServerSideEncryption = aws.String(fs.sseType)
	}

	_, err := fs.s3.PutObject(input)
	return err
}

func (fs *s3StreamStore) checkParentDir(child string) error {
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

func (fs *s3StreamStore) Remove(name string) error {
	fi, err := fs.Stat(name)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		files, err := fs.Readdir(name)
		if err != nil {
			return err
		}
		if len(files) != 0 {
			return fmt.Errorf("%s : directory not empty", name)
		}
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.fixTrailingSlash(name, fi.IsDir())),
	}
	_, err = fs.s3.DeleteObject(input)
	return err
}

func (fs *s3StreamStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
	name = fs.noSlashPrefix(name)

	if err := fs.checkParentDir(name); err != nil {
		return nil, err
	}

	if fi, err := fs.Stat(name); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	uploader := s3manager.NewUploaderWithClient(fs.s3)

	pr, pw := io.Pipe()

	input := &s3manager.UploadInput{
		Body:   pr,
		Key:    aws.String(name),
		Bucket: aws.String(fs.bucket),
	}

	if fs.sseType != "" {
		input.ServerSideEncryption = aws.String(fs.sseType)
	}

	errCh := make(chan error, 1)

	go func() {
		_, err := uploader.Upload(input)
		errCh <- err
	}()

	ul := &s3uploader{
		errCh,
		pw,
	}
	return ul, nil
}

func (fs *s3StreamStore) noSlashPrefix(s string) string {
	if strings.HasPrefix(s, "/") {
		return s[1:]
	}
	return s
}

func (fs *s3StreamStore) noSlashSuffix(s string) string {
	if strings.HasSuffix(s, "/") {
		return s[:len(s)-1]
	}
	return s
}

func (fs *s3StreamStore) fixTrailingSlash(s string, wantSlash bool) string {
	if wantSlash {
		if !strings.HasSuffix(s, "/") {
			return s + "/"
		}
	} else {
		if strings.HasSuffix(s, "/") {
			return s[0 : len(s)-1]
		}
	}
	return s
}

func (fs *s3StreamStore) lastElem(s string) string {
	_, f := filepath.Split(fs.noSlashSuffix(s))
	return f
}

type s3uploader struct {
	errCh chan error
	wc    io.WriteCloser
}

func (wc *s3uploader) Write(data []byte) (int, error) {
	return wc.wc.Write(data)
}

func (wc *s3uploader) Close() error {
	err := wc.wc.Close()
	if err != nil {
		return err
	}
	return <-wc.errCh
}

func (fs *s3StreamStore) Readdir(name string) ([]os.FileInfo, error) {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}
	if strings.HasPrefix(name, "/") {
		name = name[1:]
	}

	var results []os.FileInfo

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(name),
		Delimiter: aws.String("/"),
	}
	for {
		out, err := fs.s3.ListObjectsV2(input)
		if err != nil {
			return nil, err
		}
		for _, content := range out.Contents {
			if *content.Key != name {
				result := &s3StatResult{
					name:    strings.TrimPrefix(*content.Key, name),
					modTime: *content.LastModified,
					size:    *content.Size,
				}
				results = append(results, result)
			}
		}
		for _, prefix := range out.CommonPrefixes {
			result := &s3StatResult{
				name:  fs.noSlashSuffix(strings.TrimPrefix(*prefix.Prefix, name)), // a bit confusing because prefix is used in different contexts here.
				isDir: true,
				// modTime: ??
				size: 4096,
			}
			results = append(results, result)
		}

		if !*out.IsTruncated {
			sort.Slice(results, func(i, j int) bool { return results[i].Name() < results[j].Name() })
			return results, nil
		}

		input.ContinuationToken = out.NextContinuationToken
	}
}
