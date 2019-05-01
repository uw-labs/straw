package straw

import (
	"fmt"
	"io"
	"log"
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
)

type ServerSideEncryptionType string

const (
	ServerSideEncryptionTypeNone   ServerSideEncryptionType = ""
	ServerSideEncryptionTypeAES256 ServerSideEncryptionType = "AES256"
)

var _ StreamStore = &S3StreamStore{}

func NewS3StreamStore(bucket string, options ...S3Option) (*S3StreamStore, error) {

	sess, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)

	ss := &S3StreamStore{
		sess:   sess,
		s3:     svc,
		bucket: bucket,
	}

	for _, option := range options {
		switch opt := option.(type) {
		case serverSideEncryptionOpt:
			ss.sseType = ServerSideEncryptionType(opt)
		default:
			log.Fatalf("unhandled option type %T. This is a bug.", opt)
		}
	}

	return ss, nil
}

type S3StreamStore struct {
	sess    *session.Session
	s3      *s3.S3
	bucket  string
	sseType ServerSideEncryptionType
}

func (fs *S3StreamStore) Lstat(name string) (os.FileInfo, error) {
	panic("write me")
}

func (fs *S3StreamStore) Stat(name string) (os.FileInfo, error) {
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

	//log.Printf("Stat: out is %#v\n", out)

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
				//modTime: ??,
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

func (fs *S3StreamStore) OpenReadCloser(name string) (StrawReader, error) {

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
		log.Printf("WARN: unhandled error type :  %T\n", err)
		return nil, err
	}
	return out.Body, nil
}

func (fs *S3StreamStore) Mkdir(name string, mode os.FileMode) error {
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

	if fs.sseType != ServerSideEncryptionTypeNone {
		input.ServerSideEncryption = aws.String(string(fs.sseType))
	}

	_, err := fs.s3.PutObject(input)
	return err
}

func (fs *S3StreamStore) checkParentDir(child string) error {
	child = fs.noSlashPrefix(child)
	child = fs.noSlashSuffix(child)

	//log.Printf("checkParentDir : %s\n", child)
	d, _ := filepath.Split(child)
	//log.Printf("checkParentDir : d is  %s\n", d)
	if d != "" {
		//log.Printf("checkParentDir doing stat %s\n", d)
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

func (fs *S3StreamStore) Remove(name string) error {
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
			//log.Printf("dir not empty : files are %#v\n", files)
			return fmt.Errorf("%s : directory not empty", name)
		}
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.fixTrailingSlash(name, fi.IsDir())),
	}
	_, err = fs.s3.DeleteObject(input)
	//log.Printf("Remove : removed %s\nout is %#v\n", fi.Name(), out)
	return err
}

func (fs *S3StreamStore) CreateWriteCloser(name string) (StrawWriter, error) {
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

	if fs.sseType != ServerSideEncryptionTypeNone {
		input.ServerSideEncryption = aws.String(string(fs.sseType))
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

func (fs *S3StreamStore) noSlashPrefix(s string) string {
	if strings.HasPrefix(s, "/") {
		return s[1:]
	}
	return s
}

func (fs *S3StreamStore) noSlashSuffix(s string) string {
	if strings.HasSuffix(s, "/") {
		return s[:len(s)-1]
	}
	return s
}

func (fs *S3StreamStore) fixTrailingSlash(s string, wantSlash bool) string {
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

func (fs *S3StreamStore) lastElem(s string) string {
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

func (fs *S3StreamStore) Readdir(name string) ([]os.FileInfo, error) {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}
	if strings.HasPrefix(name, "/") {
		name = name[1:]
	}

	//log.Printf("Readdir in %s\n", name)

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
		//log.Printf("Readdir out is %#v\n", out)
		for _, content := range out.Contents {
			if *content.Key != name {
				//log.Printf("Readdir : adding result for dir %s : %#v\n", name, *content)
				result := &s3StatResult{
					name:    strings.TrimPrefix(*content.Key, name),
					modTime: *content.LastModified,
					size:    *content.Size,
				}
				results = append(results, result)
			}
		}
		for _, prefix := range out.CommonPrefixes {
			//log.Printf("Readdir : adding result for dir %s : %#v\n", name, *prefix)
			result := &s3StatResult{
				name:  fs.noSlashSuffix(strings.TrimPrefix(*prefix.Prefix, name)), // a bit confusing because prefix is used in different contexts here.
				isDir: true,
				//modTime: ??
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
