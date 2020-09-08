// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package report

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zeebo/errs"

	"storj.io/perftester/internal/config"
)

// TextReporter gathers reports and generates a formatted text report.
type TextReporter struct {
	lock          sync.Mutex
	results       fileTestResults
	fileTestSizes map[config.ID]int
}

// endpointResults is keyed by the endpointID
type endpointResults map[config.ID]*config.Result

type operationResults map[config.Operation]endpointResults

// fileTestResults is keyed by the fileTestID
type fileTestResults map[config.ID]operationResults

// NewTextReporter creats a TextReporter.
func NewTextReporter(fileTestSizes map[config.ID]int) *TextReporter {
	return &TextReporter{
		results:       make(fileTestResults),
		fileTestSizes: fileTestSizes,
	}
}

// Report accepts a single report.
func (s *TextReporter) Report(ctx context.Context, operation config.Operation, fileTestID config.ID, endpointID config.ID, result *config.Result) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, ok := s.results[fileTestID]
	if !ok {
		s.results[fileTestID] = make(operationResults)
	}

	_, ok = s.results[fileTestID][operation]
	if !ok {
		s.results[fileTestID][operation] = make(endpointResults)
	}

	s.results[fileTestID][operation][endpointID] = result

	return nil
}

// FormatResults returns a string report of all reported results.
func (s *TextReporter) FormatResults(ctx context.Context) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return formatResults(s.fileTestSizes, s.results)
}

func formatResults(fileTestSizes map[config.ID]int, results fileTestResults) (string, error) {
	const filePrefix = "File: "
	const sectionBreak = "     "

	var reportString strings.Builder

	fileTestIDs, endpointIDs, operations := uniqueSortedIDs(results)
	for _, fileTestID := range fileTestIDs {
		stars := strings.Repeat("*", len(filePrefix)+len(fileTestID))
		writeWithBreak(&reportString, stars)
		writeWithBreak(&reportString, filePrefix+string(fileTestID))
		writeWithBreak(&reportString, stars)
		writeBreak(&reportString)

		// Build headerRow
		headerRow := []string{"Operation"}
		for _, endpointID := range endpointIDs {
			headerRow = append(headerRow, string(endpointID))
		}

		var rows [][]string
		for _, operation := range operations {
			row := []string{operation.String()}
			for _, endpointID := range endpointIDs {
				result := results[fileTestID][operation][endpointID]
				fileTestSize := fileTestSizes[fileTestID]
				if fileTestSize == 0 {
					return "", errs.New("Unknown fileTestSize for %s", string(fileTestID))
				}
				row = append(row, formatResultForRow(operation, fileTestSize, result))
			}
			rows = append(rows, row)
		}
		tableRows := [][]string{headerRow}
		for _, row := range rows {
			tableRows = append(tableRows, row)
		}
		tableStr, err := MakeTable(tableRows, "-")
		if err != nil {
			return "", err
		}
		writeWithBreak(&reportString, tableStr)
	}

	return reportString.String(), nil
}

func formatResultForRow(operation config.Operation, fileTestSize int, result *config.Result) string {
	if result == nil {
		return "-"
	}

	if result.Error != "" {
		return "ERR"
	}

	if operation == config.Delete {
		return result.Duration.String()
	}

	megabits := float64(fileTestSize) * 8 / 1000 / 1000
	seconds := result.Duration.Seconds()
	return fmt.Sprintf("%s Mbps", strconv.FormatFloat(megabits/seconds, 'f', 2, 64))
}

func uniqueSortedIDs(results fileTestResults) (fileTestIDs []config.ID, endpointIDs []config.ID, operations []config.Operation) {
	var (
		seenFileTestIDs = make(map[config.ID]struct{})
		seenEndpointIDs = make(map[config.ID]struct{})
		seenOperations  = make(map[config.Operation]struct{})
	)

	for fileTestID, operationResults := range results {
		_, ok := seenFileTestIDs[fileTestID]
		if !ok {
			fileTestIDs = append(fileTestIDs, fileTestID)
			seenFileTestIDs[fileTestID] = struct{}{}
		}

		for operation, endpointResults := range operationResults {
			_, ok := seenOperations[operation]
			if !ok {
				operations = append(operations, operation)
				seenOperations[operation] = struct{}{}
			}
			for endpointID := range endpointResults {
				_, ok := seenEndpointIDs[endpointID]
				if !ok {
					endpointIDs = append(endpointIDs, endpointID)
					seenEndpointIDs[endpointID] = struct{}{}
				}
			}
		}
	}

	sortDescendingFunc := func(slice []config.ID) func(i, j int) bool {
		return func(i, j int) bool {
			return slice[i] < slice[j]
		}
	}

	sort.Slice(fileTestIDs, sortDescendingFunc(fileTestIDs))
	sort.Slice(endpointIDs, sortDescendingFunc(endpointIDs))
	sort.Slice(operations, func(i, j int) bool { return operations[i] < operations[j] })

	return fileTestIDs, endpointIDs, operations
}

func writeWithBreak(builder *strings.Builder, s string) {
	builder.WriteString(s)
	writeBreak(builder)
}

func writeBreak(builder *strings.Builder) {
	builder.WriteRune('\n')
}

// MakeTable creates a formatted test table based on the rows provided.
func MakeTable(rows [][]string, headerSeperator string) (string, error) {
	const padding = 5
	var numColumns int
	var table strings.Builder

	maxColumnLenghts := make(map[int]int)
	for _, row := range rows {
		if numColumns == 0 {
			numColumns = len(row)
		}
		if len(row) != numColumns {
			return "", errs.New("Mismatched column numbers")
		}

		for i, item := range row {
			if len(item) > maxColumnLenghts[i] {
				maxColumnLenghts[i] = len(item)
			}
		}
	}

	for i, row := range rows {
		for i, item := range row {
			var postItem string

			if i < len(row)-1 {
				// Add padding. We default to 5 spaces after the longest item.
				postItem = strings.Repeat(" ", maxColumnLenghts[i]-len(item)+padding)
			} else {
				// If it's the last item in the list add a line break.
				postItem = "\n"
			}
			table.WriteString(item + postItem)
		}

		// If we just wrote the first row, now write the header separator.
		if i == 0 && headerSeperator != "" {
			totalLength := 0
			totalLength += (numColumns - 1) * padding // add padding for all but the last item
			for _, maxLength := range maxColumnLenghts {
				totalLength += maxLength
			}
			writeWithBreak(&table, strings.Repeat(headerSeperator, totalLength))
		}
	}

	return table.String(), nil
}
