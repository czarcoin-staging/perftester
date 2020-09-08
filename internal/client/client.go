// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package client

import (
	"context"
	"io"
)

// Client represents a storage client.
type Client interface {
	List(ctx context.Context, prefix string, recursive bool) (obj []*ListObject, err error)
	Upload(ctx context.Context, name string, strm io.Reader) (err error)
	Download(ctx context.Context, name string) (strm io.ReadCloser, err error)
	Delete(ctx context.Context, name string) (err error)
	IP(ctx context.Context) (addr string, err error)
	Close() (err error)
}

// ListObject is an object type that can be used by any client.
type ListObject struct {
	Key   string
	IsPre bool
}
