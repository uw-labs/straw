Straw
=====

[![go-doc](https://godoc.org/github.com/uw-labs/straw?status.svg)](https://godoc.org/github.com/uw-labs/straw)

Straw is a filesystem abstraction for Go. It started life as simply supporting streams (no seek) but evolved to a more complete API over time.

Currently it supports local filesystem aws s3, and sftp as storage options.

WARNING : The API is not stable at this point.

For the subset of filesystem-like functionality that it does provide, it aims to remain close to the existing Go standard library types and concepts as possible.
