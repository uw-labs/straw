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
	// the only "built in" backend is "file"
	Register("file", func(u *url.URL) (StreamStore, error) {
		return &osStreamStore{}, nil
	})
}
