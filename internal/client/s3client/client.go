// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package s3

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	monkit "github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	cli "storj.io/perftester/internal/client"
	"storj.io/perftester/internal/config"
)

var (
	mon = monkit.Package()

	// Error is the error for this package.
	Error = errs.Class("s3-client")
)

// Client is an S3 client.
type Client struct {
	cfg     config.S3Endpoint
	session *session.Session
}

// New creates a new S3 client.
func New(cfg config.S3Endpoint) (*Client, error) {
	switch {
	case cfg.Region == "":
		return nil, errs.New("region is required")
	case cfg.Bucket == "":
		return nil, errs.New("bucket is required")
	case cfg.AccessKey == "":
		return nil, errs.New("access key is required")
	case cfg.SecretKey == "":
		return nil, errs.New("secret key is required")
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
		Endpoint:    aws.String(cfg.Address),
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		session: sess,
		cfg:     cfg,
	}, nil
}

// List returns the objects found at name.
func (client *Client) List(ctx context.Context, name string, recursive bool) (objs []*cli.ListObject, err error) {
	defer mon.Task()(&ctx)(&err)

	svc := s3.New(client.session)

	path := client.bucketKey(name)
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}

	var delimeter *string
	if !recursive {
		delimeter = aws.String("/")
	}

	out, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(client.cfg.Bucket),
		Prefix:    aws.String(path),
		Delimiter: delimeter,
	})
	if err != nil {
		return nil, err
	}

	for _, pre := range out.CommonPrefixes {
		objs = append(objs, &cli.ListObject{
			Key:   pre.String(),
			IsPre: true,
		})
	}

	for _, obj := range out.Contents {
		objs = append(objs, &cli.ListObject{
			Key:   obj.String(),
			IsPre: false,
		})
	}

	return objs, nil
}

// Upload uploads to S3.
func (client *Client) Upload(ctx context.Context, name string, strm io.Reader) (err error) {
	defer mon.Task()(&ctx)(&err)

	// Use a new uploader for each upload so we don't skew results with the
	// caching done by the uploader part pool.
	uploader := s3manager.NewUploader(client.session)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(client.cfg.Bucket),
		Key:    aws.String(client.bucketKey(name)),
		Body:   strm,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file %q: %v", name, err)
	}
	return nil
}

// Download downloads from S3.
func (client *Client) Download(ctx context.Context, name string) (strm io.ReadCloser, err error) {
	defer mon.Task()(&ctx)(&err)

	svc := s3.New(client.session)

	out, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(client.cfg.Bucket),
		Key:    aws.String(client.bucketKey(name)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file %q: %v", name, err)
	}
	return out.Body, nil
}

// Delete deletes from S3.
func (client *Client) Delete(ctx context.Context, name string) (err error) {
	defer mon.Task()(&ctx)(&err)

	svc := s3.New(client.session)

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(client.cfg.Bucket),
		Key:    aws.String(client.bucketKey(name)),
	})
	if err != nil {
		return fmt.Errorf("failed to delete file %q: %v", name, err)
	}
	return nil
}

// IP returns the IP address of the endpoint.
func (client *Client) IP(ctx context.Context) (addr string, err error) {
	// it's impossible to get A IP address from s3 endpoint since it has a range of IPs
	// we will skip it for now.
	return "", err
}

// Close closes the client.
func (client *Client) Close() (err error) { return nil }

func (client *Client) bucketKey(name string) string {
	if client.cfg.Path != "" {
		return path.Join(client.cfg.Path, name)
	}
	return name
}
