// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package storjclient

import (
	"context"
	"io"
	"net"
	"strings"

	monkit "github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/storj"
	cli "storj.io/perftester/internal/client"
	"storj.io/perftester/internal/config"
	"storj.io/uplink"
)

var (
	mon = monkit.Package()

	// Error is the error for this package.
	Error = errs.Class("storj-client")
)

// Client is a storj client.
type Client struct {
	cfg     config.StorjEndpoint
	address string

	project *uplink.Project
}

// New creates a new storj client.
func New(ctx context.Context, log *zap.Logger, cfg config.StorjEndpoint) (*Client, error) {
	access, err := uplink.ParseAccess(cfg.Access)
	if err != nil {
		return nil, err
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	_, err = project.EnsureBucket(ctx, cfg.Bucket)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	satelliteAddress, err := parseSatelliteAddressFromScope(cfg.Access)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &Client{
		cfg:     cfg,
		address: satelliteAddress,
		project: project,
	}, nil
}

// Address returns the client address.
func (client *Client) Address() string {
	return client.address
}

// List returns the objects found at prefix.
func (client *Client) List(ctx context.Context, name string, recursive bool) (objs []*cli.ListObject, err error) {
	defer mon.Task()(&ctx)(&err)

	if name != "" && !strings.HasSuffix(name, "/") {
		name += "/"
	}
	path := client.joinWithClientPath(name)
	objects := client.project.ListObjects(ctx, client.cfg.Bucket, &uplink.ListObjectsOptions{
		Prefix:    path,
		Recursive: recursive,
		System:    true,
	})

	for objects.Next() {
		item := objects.Item()
		objs = append(objs, &cli.ListObject{
			Key:   item.Key,
			IsPre: item.IsPrefix,
		})
	}
	return objs, objects.Err()
}

// Upload uploads to storj.
func (client *Client) Upload(ctx context.Context, name string, strm io.Reader) (err error) {
	defer mon.Task()(&ctx)(&err)

	upload, err := client.project.UploadObject(ctx, client.cfg.Bucket, client.joinWithClientPath(name), nil)
	if err != nil {
		return Error.Wrap(err)
	}

	_, err = io.Copy(upload, strm)
	if err != nil {
		aborterr := upload.Abort()
		return Error.Wrap(errs.Combine(err, aborterr))
	}

	err = upload.Commit()
	return Error.Wrap(err)
}

// Download downloads from storj.
func (client *Client) Download(ctx context.Context, name string) (stream io.ReadCloser, err error) {
	defer mon.Task()(&ctx)(&err)

	download, err := client.project.DownloadObject(ctx, client.cfg.Bucket, client.joinWithClientPath(name), nil)
	if err != nil {
		return nil, Error.New("could not open object at %q/%q: %v", client.cfg.Bucket, name, err)
	}

	return download, nil
}

// Delete deletes from storj.
func (client *Client) Delete(ctx context.Context, name string) (err error) {
	defer mon.Task()(&ctx)(&err)

	// Initiate a download of the same object again
	if _, err := client.project.DeleteObject(ctx, client.cfg.Bucket, client.joinWithClientPath(name)); err != nil {
		return Error.New("could not delete object at %q/%q: %v", client.cfg.Bucket, name, err)
	}
	return nil
}

// IP returns the IP address of the endpoint.
func (client *Client) IP(ctx context.Context) (string, error) {
	nodeURL, err := storj.ParseNodeURL(client.address)
	if err != nil {
		return "", err
	}

	addr, _, err := net.SplitHostPort(nodeURL.Address)
	if err != nil {
		return "", err
	}

	return addr, nil
}

// Close closes the client.
func (client *Client) Close() (err error) {
	return client.project.Close()
}

func (client *Client) joinWithClientPath(path string) string {
	if client.cfg.Path == "" {
		return path
	}
	return storj.JoinPaths(client.cfg.Path, path)
}
