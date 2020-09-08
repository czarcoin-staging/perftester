// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/perftester/internal/check"
	s3 "storj.io/perftester/internal/client/s3client"
	"storj.io/perftester/internal/client/storjclient"
	"storj.io/perftester/internal/config"
	"storj.io/perftester/internal/report"
	"storj.io/private/cfgstruct"
	"storj.io/private/process"
)

var cfg struct {
	ConfigPath string `default:"config.toml" help:"configuration file location"`
}

func main() {
	cmd := &cobra.Command{
		Use:   "perftester [flags]",
		Short: "performance tester",
		RunE:  Main,
	}
	process.Bind(cmd, &cfg, cfgstruct.DefaultsFlag(cmd))
	process.Exec(cmd)
}

// Main is the main function run
func Main(cmd *cobra.Command, _ []string) (err error) {
	// Errors returned from here result in the "usage" being shown, so only
	// the error will be logged and the program explicitly exited.
	if err := run(context.Background(), cmd); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Execution failed: %+v\n", err)
		os.Exit(1)
	}
	return nil
}

func run(ctx context.Context, cmd *cobra.Command) (err error) {
	if cfg.ConfigPath == "" {
		return errs.New("empty config path")
	}
	log, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()
	zap.ReplaceGlobals(log)

	conf, err := config.LoadConfig(cfg.ConfigPath)
	if err != nil {
		return err
	}

	var endpoints []*config.Endpoint
	for id, endpoint := range conf.Endpoints.S3 {
		client, err := s3.New(endpoint)
		if err != nil {
			return err
		}
		endpoints = append(endpoints, &config.Endpoint{
			ID:     id,
			Bucket: endpoint.Bucket,
			Path:   endpoint.Path,
			Client: client,
		})
	}
	for id, endpoint := range conf.Endpoints.Storj {
		client, err := storjclient.New(ctx, log.Named("storjclient"), endpoint)
		if err != nil {
			return err
		}
		endpoints = append(endpoints, &config.Endpoint{
			ID:     id,
			Bucket: endpoint.Bucket,
			Path:   endpoint.Path,
			Client: client,
		})
	}

	fileTestSizes := make(map[config.ID]int)
	for fileTestID, fileTest := range conf.FileTests {
		fileTestSizes[fileTestID] = int(fileTest.Size)
	}
	reporter := report.NewTextReporter(fileTestSizes)

	checker := check.NewChecker(log.Named("checker"), reporter, endpoints, conf.FileTests, conf.Timeout)
	if err := checker.RunChecks(ctx); err != nil {
		return err
	}

	report, err := reporter.FormatResults(ctx)
	if err != nil {
		return err
	}

	fmt.Print(report)
	return nil
}
