package straw

import (
	"fmt"
	"net/url"
)

func Open(u string) (StreamStore, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	backendsLk.RLock()
	defer backendsLk.RUnlock()

	f := backends[parsed.Scheme]
	if f == nil {
		return nil, fmt.Errorf("unknown scheme : %s", parsed.Scheme)
	}
	return f(parsed)
}

func init() {
	Register("file", func(u *url.URL) (StreamStore, error) {
		return &OsStreamStore{}, nil
	})
	Register("gs", func(u *url.URL) (StreamStore, error) {
		creds := u.Query().Get("credentialsfile")
		if creds == "" {
			return nil, fmt.Errorf("gs URLs must provide a `credentialsfile` parameter")
		}
		return NewGCSStreamStore(creds, u.Host)
	})
	Register("s3", func(u *url.URL) (StreamStore, error) {
		sse := u.Query().Get("sse")
		var opts []S3Option
		switch sse {
		case "":
		case "AES256":
			opts = append(opts, S3ServerSideEncoding(ServerSideEncryptionTypeAES256))
		default:
			return nil, fmt.Errorf("unknown server side encryption type '%s'", sse)
		}
		return NewS3StreamStore(u.Host, opts...)
	})
	Register("sftp", func(u *url.URL) (StreamStore, error) {
		return NewSFTPStreamStore(u.String())
	})
}
