// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package check

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/perftester/internal/config"
)

// reporter interface is used to handle reports for each operation as they finish.
type reporter interface {
	Report(ctx context.Context, operation config.Operation, fileTestID config.ID, endpointID config.ID, result *config.Result) error
}

// Checker can run various performance tests.
type Checker struct {
	log       *zap.Logger
	endpoints []*config.Endpoint
	fileTests map[config.ID]config.FileTest
	timeout   config.Duration
	reporter  reporter
}

// NewChecker creates a new checker.
func NewChecker(log *zap.Logger, reporter reporter, endpoints []*config.Endpoint, fileTests map[config.ID]config.FileTest, timeout config.Duration) *Checker {
	return &Checker{
		endpoints: endpoints,
		fileTests: fileTests,
		timeout:   timeout,
		reporter:  reporter,
		log:       log,
	}
}

// RunChecks runs all operations on all files.
func (c *Checker) RunChecks(ctx context.Context) error {
	// Run all checks on all endpoints
	for fileTestID, fileTest := range c.fileTests {
		for _, endpoint := range c.endpoints {
			err := c.RunCheck(ctx, fileTestID, fileTest, endpoint)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// RunCheck runs all operations on a single file and endpoint.
func (c *Checker) RunCheck(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint) error {
	c.log.Info("Starting check", zap.String("fileTestID", string(fileTestID)), zap.String("endpointID", string(endpoint.ID)))

	if fileTest.Timeout <= 0 {
		fileTest.Timeout = c.timeout
	}

	if fileTest.Seed <= 0 {
		fileTest.Seed = time.Now().UnixNano()
	}

	if fileTest.NumParallel <= 0 {
		fileTest.NumParallel = 1
	}

	c.log.Info("Upload", zap.String("fileTestID", string(fileTestID)), zap.String("endpointID", string(endpoint.ID)))
	err := c.Upload(ctx, fileTestID, fileTest, endpoint)
	if err != nil {
		return err
	}

	c.log.Info("Download", zap.String("fileTestID", string(fileTestID)), zap.String("endpointID", string(endpoint.ID)))
	err = c.Download(ctx, fileTestID, fileTest, endpoint)
	if err != nil {
		return err
	}

	c.log.Info("Delete", zap.String("fileTestID", string(fileTestID)), zap.String("endpointID", string(endpoint.ID)))
	err = c.Delete(ctx, fileTestID, fileTest, endpoint)
	if err != nil {
		return err
	}

	return nil
}

// Upload makes an upload check.
func (c *Checker) Upload(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint) error {
	result := newResultNow()
	err := upload(ctx, fileTestID, fileTest, endpoint)
	result.Duration = time.Since(result.StartTime)
	result.Success = err == nil
	if err != nil {
		c.log.Error("Upload failed", zap.Error(err), zap.String("fileTestID", string(fileTestID)), zap.String("endpoint", string(endpoint.ID)))
		result.Error = err.Error()
	}
	return c.reporter.Report(ctx, config.Upload, fileTestID, endpoint.ID, result)
}

func upload(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint) (err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(fileTest.Timeout))
	defer cancel()
	return runParallel(ctx, int(fileTest.NumParallel), func(i int) error {
		r := fileReader(fileTest, i)
		return endpoint.Client.Upload(ctx, pathName(fileTestID, i), r)
	})
}

// Delete makes a delete check.
func (c *Checker) Delete(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint) error {
	result := newResultNow()
	err := del(ctx, fileTestID, fileTest, endpoint)
	result.Duration = time.Since(result.StartTime)
	result.Success = err == nil
	if err != nil {
		c.log.Error("Delete failed", zap.Error(err), zap.String("fileTestID", string(fileTestID)), zap.String("endpoint", string(endpoint.ID)))
		result.Error = err.Error()
	}
	return c.reporter.Report(ctx, config.Delete, fileTestID, endpoint.ID, result)
}

func del(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint) (err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(fileTest.Timeout))
	defer cancel()
	return runParallel(ctx, int(fileTest.NumParallel), func(i int) error {
		return endpoint.Client.Delete(ctx, pathName(fileTestID, i))
	})
}

// Download runs the download check for a single fileTest and endpoint.
func (c *Checker) Download(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint) error {
	expectedHashes := make([][]byte, 0, fileTest.NumParallel)
	for i := 0; i < int(fileTest.NumParallel); i++ {
		r := fileReader(fileTest, i)
		expectedHash := sha256.New()
		_, err := io.Copy(expectedHash, r)
		if err != nil {
			return err
		}
		expectedHashes = append(expectedHashes, expectedHash.Sum(nil))
	}

	result := newResultNow()
	err := download(ctx, fileTestID, fileTest, endpoint, expectedHashes)
	result.Duration = time.Since(result.StartTime)
	result.Success = err == nil
	if err != nil {
		c.log.Error("Download failed", zap.Error(err), zap.String("fileTestID", string(fileTestID)), zap.String("endpoint", string(endpoint.ID)))
		result.Error = err.Error()
	}
	return c.reporter.Report(ctx, config.Download, fileTestID, endpoint.ID, result)
}

func download(ctx context.Context, fileTestID config.ID, fileTest config.FileTest, endpoint *config.Endpoint, expectedHashes [][]byte) (err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(fileTest.Timeout))
	defer cancel()
	return runParallel(ctx, int(fileTest.NumParallel), func(i int) error {
		hash := sha256.New()

		strm, err := endpoint.Client.Download(ctx, pathName(fileTestID, i))
		if err != nil {
			return err
		}
		defer func() { err = errs.Combine(err, strm.Close()) }()

		_, err = io.Copy(hash, strm)
		if err != nil {
			return err
		}

		digest := hash.Sum(nil)
		if !bytes.Equal(digest, expectedHashes[i]) {
			return errs.New("unexpected %q/%d file contents: expected sha256 digest %x; got %x", fileTestID, i, expectedHashes[i], digest)
		}

		return nil
	})
}

func runParallel(ctx context.Context, numParallel int, f func(i int) error) error {
	var eg errgroup.Group
	for i := 0; i < numParallel; i++ {
		func(i int) {
			eg.Go(func() error {
				return f(i)
			})
		}(i)
	}
	return eg.Wait()
}

func pathName(id config.ID, i int) string {
	return string(id) + strconv.Itoa(i)
}

func fileReader(fileTest config.FileTest, i int) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(fileTest.Seed+int64(i))), fileTest.Size)
}

// newResultNow returns a Result with the Time value set to now.
func newResultNow() *config.Result {
	return &config.Result{
		StartTime: time.Now(),
	}
}
