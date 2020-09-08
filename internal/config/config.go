// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package config

import (
	"time"

	"github.com/BurntSushi/toml"

	"storj.io/perftester/internal/client"
)

// An ID is any arbitrary sring
type ID string

// Config is the config for the tests.
type Config struct {
	FileTests  map[ID]FileTest `toml:"filetest"`
	Endpoints  Endpoints       `toml:"endpoint"`
	Monitoring Monitoring
	Timeout    Duration
}

// FileTest defines a test to run on a file.
type FileTest struct {
	NumParallel int64    `toml:"numparallel"`
	Timeout     Duration `toml:"timeout"`
	Size        int64    `toml:"size"` // Size to test in bytes.
	Seed        int64    `toml:"seed"` // Custom seed to make file unique.
}

// Endpoints is a collection of remote endpoints.
type Endpoints struct {
	Storj map[ID]StorjEndpoint `toml:"storj"`
	S3    map[ID]S3Endpoint    `toml:"s3"`
}

// Endpoint is a generic endpoint.
type Endpoint struct {
	ID     ID
	Bucket string
	Path   string
	Client client.Client
}

// StorjEndpoint represents a storj endpoint.
type StorjEndpoint struct {
	Access string `toml:"access"`
	Bucket string `toml:"bucket"`
	Path   string `toml:"path"`
	Client client.Client
}

// S3Endpoint is the represents an S3 endpoint.
type S3Endpoint struct {
	Region    string `toml:"region"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
	Path      string `toml:"path"`
	Address   string `toml:"address"`
	Client    client.Client
}

// Monitoring is the monitoring config information.
type Monitoring struct {
	Address    string `toml:"address"`
	InstanceID string `toml:"instance_id"`
	TracingURL string `toml:"tracing_url"`
}

// Duration assists in parsing duration data in the toml file.
type Duration time.Duration

// UnmarshalText unmarshals the duration text from toml.
func (d *Duration) UnmarshalText(data []byte) error {
	duration, err := time.ParseDuration(string(data))
	if err == nil {
		*d = Duration(duration)
	}
	return err
}

// LoadConfig loads the toml config
func LoadConfig(path string) (config Config, err error) {
	_, err = toml.DecodeFile(path, &config)
	return config, err
}

// Result represents a single result.
type Result struct {
	StartTime time.Time
	Duration  time.Duration
	Success   bool
	Error     string
}

// Operation represents the type of operation done for the test.
type Operation int

const (
	// Upload operation.
	Upload Operation = iota
	// Download operation.
	Download
	// Delete operation.
	Delete
)

func (o Operation) String() string {
	switch o {
	case Upload:
		return "Upload"
	case Download:
		return "Download"
	case Delete:
		return "Delete"
	default:
		return ""
	}
}
