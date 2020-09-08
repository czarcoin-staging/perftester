// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package report_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"storj.io/common/testcontext"
	"storj.io/perftester/internal/config"
	"storj.io/perftester/internal/report"
)

type reportTest struct {
	operation  config.Operation
	fileTestID config.ID
	endpointID config.ID
	result     *config.Result
}

func TestTextReporter(t *testing.T) {
	// - Result with error
	// - missing endpointID
	// - missing fileTestID
	ctx := testcontext.New(t)
	tests := []struct {
		fileTestSizes map[config.ID]int
		reports       []*reportTest
		expected      string
	}{
		{
			fileTestSizes: map[config.ID]int{
				"ft1": 10000000,
				"ft2": 20000000,
			},
			expected: `*********
File: ft1
*********

Operation     end1           end2
---------------------------------------
Upload        16.00 Mbps     10.00 Mbps
Download      -              10.00 Mbps

*********
File: ft2
*********

Operation     end1     end2
---------------------------------
Upload        -        -
Download      -        20.00 Mbps

`,
			reports: []*reportTest{
				{
					operation:  config.Upload,
					fileTestID: "ft1",
					endpointID: "end1",
					result: &config.Result{
						Duration: 5 * time.Second,
						Success:  true,
						Error:    "",
					},
				},
				{
					operation:  config.Upload,
					fileTestID: "ft1",
					endpointID: "end2",
					result: &config.Result{
						Duration: 8 * time.Second,
						Success:  true,
						Error:    "",
					},
				},
				{
					operation:  config.Download,
					fileTestID: "ft1",
					endpointID: "end2",
					result: &config.Result{
						Duration: 8 * time.Second,
						Success:  true,
						Error:    "",
					},
				},
				{
					operation:  config.Download,
					fileTestID: "ft2",
					endpointID: "end2",
					result: &config.Result{
						Duration: 8 * time.Second,
						Success:  true,
						Error:    "",
					},
				},
			},
		},
		{
			fileTestSizes: map[config.ID]int{
				"ft1": 10000000,
			},
			expected: `*********
File: ft1
*********

Operation     end1
------------------
Upload        ERR

`,
			reports: []*reportTest{
				{
					operation:  config.Upload,
					fileTestID: "ft1",
					endpointID: "end1",
					result: &config.Result{
						Duration: 5 * time.Second,
						Success:  true,
						Error:    "Here is an error",
					},
				},
			},
		},
	}

	for _, test := range tests {
		reporter := report.NewTextReporter(test.fileTestSizes)
		var eg errgroup.Group
		for _, rt := range test.reports {
			// Call Report concurrently just to ensure it works.
			func(rt *reportTest) {
				eg.Go(func() error {
					return reporter.Report(ctx, rt.operation, rt.fileTestID, rt.endpointID, rt.result)
				})
			}(rt)
		}
		require.NoError(t, eg.Wait())
		str, err := reporter.FormatResults(ctx)
		require.NoError(t, err)
		assert.Equal(t, test.expected, str)
	}
}

func TestMakeTable(t *testing.T) {
	tests := []struct {
		rows            [][]string
		headerSeperator string
		expected        string
	}{
		{
			rows: [][]string{
				{"this", "is", "a", "header"},
				{"here", "we", "find", "row2"},
				{"different", "items", "a", "b"},
			},
			headerSeperator: "-",
			expected: `this          is        a        header
---------------------------------------
here          we        find     row2
different     items     a        b
`,
		},
		{
			rows: [][]string{
				{"this", "is", "a", "header"},
				{"here", "we", "find", "row2"},
				{"different", "items", "a", "b"},
			},
			headerSeperator: "i",
			expected: `this          is        a        header
iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii
here          we        find     row2
different     items     a        b
`,
		},
		{
			rows: [][]string{
				{"this", "is", "a", "header"},
				{"here", "we", "find", "row2"},
				{"different", "items", "a", "b"},
			},
			headerSeperator: "",
			expected: `this          is        a        header
here          we        find     row2
different     items     a        b
`,
		},
	}

	for _, test := range tests {
		table, err := report.MakeTable(test.rows, test.headerSeperator)
		require.NoError(t, err)

		require.Equal(t, test.expected, table)
	}
}
