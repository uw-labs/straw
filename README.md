Straw
=====

Straw is a streaming I/O abstraction, intended to provide a nice way to interact with various stream (blob) storage backends.

Currently it supports local filesystem and aws s3 as storage options.

It is not intended to be a general purpose VFS style API, instead focussing on cleanly and portably supporting streaming reads and streaming writes of entire objects.

For the subset of filesystem-like functionality that it does provide, it aims to remain close to the existing Go standard library types and concepts as possible.
