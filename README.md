Straw
=====

[![go-doc](https://godoc.org/github.com/uw-labs/straw?status.svg)](https://godoc.org/github.com/uw-labs/straw)


Straw is a streaming I/O abstraction, intended to provide a nice way to interact with various stream (blob) storage backends.

Currently it supports local filesystem aws s3, and sftp as storage options.

It is not intended to be a general purpose VFS style API, instead focussing on cleanly and portably supporting streaming reads and streaming writes of entire objects.

WARNING : The API is not stable at this point.

For the subset of filesystem-like functionality that it does provide, it aims to remain close to the existing Go standard library types and concepts as possible.
