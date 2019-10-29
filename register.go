package straw

import (
	"net/url"
	"sync"
)

var (
	backendsLk sync.RWMutex
	backends   = make(map[string]func(url *url.URL) (StreamStore, error))
)

func Register(scheme string, sinkFunc func(url *url.URL) (StreamStore, error)) {
	backendsLk.Lock()
	defer backendsLk.Unlock()
	if sinkFunc == nil {
		panic("straw: sink function is nil")
	}
	if _, dup := backends[scheme]; dup {
		panic("straw: RegisterSink called more than once for sink " + scheme)
	}
	backends[scheme] = sinkFunc
}
