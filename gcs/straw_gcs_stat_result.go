package gcs

import (
	"os"
	"time"
)

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
