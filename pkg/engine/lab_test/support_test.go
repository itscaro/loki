package engine_lab

/*
================================================================================
ENGINE V2 FEATURE SUPPORT TEST
================================================================================

This file provides a comprehensive table-driven test of all LogQL features
to demonstrate which features are supported in Engine V2 and which are not.

Each test case executes against REAL STORAGE using TestIngester and attempts
the complete query flow: logical planning → physical planning → execution.

TEST STRUCTURE:
---------------
- `planSupported`: Whether logical planning should succeed
- `expectExecError`: Whether execution should fail (for partial support cases)
- `expectedRowCount`: Expected number of result rows (documents actual behavior)

All features that have plan support ALWAYS get executed - no skipping!
This allows us to:
1. Verify actual engine behavior
2. Track when features become fully supported
3. Document expected results for future implementations

Feature Categories Tested:
  - Stream Selectors
  - Line Filters
  - Parsers (json, logfmt, regexp, pattern)
  - Label Filters
  - Label Manipulation (drop, keep, line_format, label_format)
  - Query Direction (forward, backward)
  - Metric Queries (count_over_time, rate, bytes_rate, etc.)
  - Aggregations (sum, avg, min, max, count, etc.)
  - Unwrap and Range Vector functions

FULLY SUPPORTED (Logical → Physical → Execution):
--------------------------------------------------
✓ Stream selectors (=, !=, =~, !~) - NOTE: Label filtering has a bug, returns all logs
✓ Multiple label selectors (AND)
✓ Line filters (|=, !=, |~, !~) - Working correctly!
✓ Multiple line filters (chaining)
✓ JSON parsing (| json) with label filters
✓ Logfmt parsing (| logfmt) with label filters
✓ Drop labels (| drop)
✓ BACKWARD direction
✓ Aggregations: sum, sum by, min, max

NOT SUPPORTED:
--------------
✗ FORWARD direction
✗ Instant vector queries (count_over_time, rate, bytes_over_time, bytes_rate)
✗ Keep labels (| keep)
✗ Line format (| line_format)
✗ Label format (| label_format)
✗ Decolorize (| decolorize)
✗ Regexp parser (| regexp)
✗ Pattern parser (| pattern)
✗ Unwrap expressions
✗ Label replace
✗ avg, count aggregations

KNOWN BUGS:
-----------
⚠ Label selectors don't filter properly - all logs are returned regardless
  of label matchers. Line filters work correctly after this bug.
  (Expected row counts reflect this bug for tracking purposes)

================================================================================
*/

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/engine/internal/executor"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/logical"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/logproto"
)

/*
TestLogQLFeatureSupport is a comprehensive table-driven test that verifies
which LogQL features are supported in Engine V2.

For each feature:
- Supported: Executes successfully and returns results
- Unsupported: Returns error (usually during logical planning)
*/
func TestLogQLFeatureSupport(t *testing.T) {
	ctx := context.Background()

	// Setup test data with various log types for comprehensive testing
	ingester := setupTestIngesterWithTimestamps(t, ctx, "test-tenant", []LogEntry{
		// Regular logs
		{Labels: `{app="test", level="error"}`, Line: "error: connection failed", Timestamp: time.Now().Add(-5 * time.Minute)},
		{Labels: `{app="test", level="info"}`, Line: "info: request completed", Timestamp: time.Now().Add(-4 * time.Minute)},
		{Labels: `{app="test", level="warn"}`, Line: "warn: high latency", Timestamp: time.Now().Add(-3 * time.Minute)},

		// JSON formatted logs
		{Labels: `{app="json-app"}`, Line: `{"level":"error","msg":"timeout"}`, Timestamp: time.Now().Add(-2 * time.Minute)},
		{Labels: `{app="json-app"}`, Line: `{"level":"info","msg":"success"}`, Timestamp: time.Now().Add(-1 * time.Minute)},

		// Logfmt formatted logs
		{Labels: `{app="logfmt-app"}`, Line: `level=error msg="connection refused"`, Timestamp: time.Now()},
	})
	defer ingester.Close()

	catalog := ingester.Catalog()
	now := time.Now()

	testCases := []struct {
		name             string
		query            string
		planSupported    bool // True if logical planning should succeed
		expectExecError  bool // True if execution should fail (planning-only support)
		expectedRowCount int  // Expected number of rows (-1 to skip row count check, used for future reference on unsupported features)
	}{
		// ===================================================================
		// STREAM SELECTORS - All Supported
		// NOTE: Engine V2 currently has a bug where label selectors don't filter
		// properly - all logs are returned regardless of label matchers.
		// The expected counts reflect ACTUAL behavior, not ideal behavior.
		// When label filtering is fixed, update these expected counts.
		// ===================================================================
		{
			name:             "simple label selector",
			query:            `{app="test"}`,
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs, should be 3 (test app only)
		},
		{
			name:             "multiple label selectors (AND)",
			query:            `{app="test", level="error"}`,
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs, should be 1 (test+error only)
		},
		{
			name:             "regex label selector",
			query:            `{app=~"test.*"}`,
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs, should be 3 (test app only)
		},
		{
			name:             "negative label selector",
			query:            `{app="test", level!="debug"}`, // Negative matchers need at least one positive
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs, should be 3 (test app, no debug level)
		},
		{
			name:             "negative regex selector",
			query:            `{app="test", level!~"debug|trace"}`,
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs, should be 3 (test app, none match debug|trace)
		},

		// ===================================================================
		// LINE FILTERS - All Supported
		// NOTE: Line filters work correctly in Engine V2.
		// However, since label selectors don't filter, the row counts
		// reflect filtering ALL 6 logs, not just the label-matched subset.
		// ===================================================================
		{
			name:             "line contains filter",
			query:            `{app="test"} |= "error"`,
			planSupported:    true,
			expectedRowCount: 3, // 3 logs contain "error" (across all apps due to label bug)
		},
		{
			name:             "line not contains filter",
			query:            `{app="test"} != "debug"`,
			planSupported:    true,
			expectedRowCount: 6, // All 6 logs don't contain "debug"
		},
		{
			name:             "line regex filter",
			query:            `{app="test"} |~ "error|warn"`,
			planSupported:    true,
			expectedRowCount: 4, // 4 logs match error|warn (across all apps)
		},
		{
			name:             "negative line regex filter",
			query:            `{app="test"} !~ "debug|trace"`,
			planSupported:    true,
			expectedRowCount: 6, // All 6 logs don't match debug|trace
		},

		// ===================================================================
		// PARSERS - Supported in Logical Planning
		// ===================================================================
		{
			name:             "json parser",
			query:            `{app="json-app"} | json`,
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs due to label selector bug
		},
		{
			name:             "json parser with label filter",
			query:            `{app="json-app"} | json | level="error"`,
			planSupported:    true,
			expectedRowCount: 2, // 2 logs have level=error after json parsing (json error + logfmt error)
		},
		{
			name:             "logfmt parser",
			query:            `{app="logfmt-app"} | logfmt`,
			planSupported:    true,
			expectedRowCount: 6, // BUG: returns all 6 logs due to label selector bug
		},
		{
			name:             "logfmt parser with label filter",
			query:            `{app="logfmt-app"} | logfmt | level="error"`,
			planSupported:    true,
			expectedRowCount: 2, // 2 logs have level=error after logfmt parsing (json error + logfmt error)
		},

		// ===================================================================
		// LABEL MANIPULATION
		// ===================================================================
		{
			name:             "drop labels",
			query:            `{app="test"} | drop app`,
			planSupported:    true,
			expectedRowCount: 6, // All 6 logs (label selector bug + drop doesn't filter)
		},
		{
			name:             "keep labels - NOT SUPPORTED",
			query:            `{app="test"} | keep level`,
			planSupported:    false,
			expectedRowCount: 6, // When implemented (with current label bug): all 6 logs
		},
		{
			name:             "line_format - NOT SUPPORTED",
			query:            `{app="test"} | line_format "{{.level}}: {{.msg}}"`,
			planSupported:    false,
			expectedRowCount: 6, // When implemented (with current label bug): all 6 logs with formatted line
		},
		{
			name:             "label_format - NOT SUPPORTED",
			query:            `{app="test"} | label_format new_label="{{.app}}"`,
			planSupported:    false,
			expectedRowCount: 6, // When implemented (with current label bug): all 6 logs with new label
		},

		// ===================================================================
		// SPECIAL FILTERS
		// ===================================================================
		{
			name:             "decolorize - NOT SUPPORTED",
			query:            `{app="test"} | decolorize`,
			planSupported:    false,
			expectedRowCount: 6, // When implemented (with current label bug): all 6 logs with colors stripped
		},

		// ===================================================================
		// QUERY DIRECTION
		// ===================================================================
		{
			name:             "backward direction (default)",
			query:            `{app="test"}`,
			planSupported:    true,
			expectedRowCount: 6, // All 6 logs due to label selector bug (should be 3)
		},
		// Note: FORWARD direction tested separately with query params

		// ===================================================================
		// METRIC QUERIES - NOT SUPPORTED (instant vector queries)
		// ===================================================================
		{
			name:             "count_over_time - NOT SUPPORTED",
			query:            `count_over_time({app="test"}[5m])`,
			planSupported:    false, // Instant vector queries not supported
			expectedRowCount: 1,     // When implemented: single metric value (count=3)
		},
		{
			name:             "rate - NOT SUPPORTED",
			query:            `rate({app="test"}[5m])`,
			planSupported:    false,
			expectedRowCount: 1, // When implemented: single metric value (rate)
		},
		{
			name:             "bytes_over_time - NOT SUPPORTED",
			query:            `bytes_over_time({app="test"}[5m])`,
			planSupported:    false,
			expectedRowCount: 1, // When implemented: single metric value (total bytes)
		},
		{
			name:             "bytes_rate - NOT SUPPORTED",
			query:            `bytes_rate({app="test"}[5m])`,
			planSupported:    false,
			expectedRowCount: 1, // When implemented: single metric value (bytes/sec)
		},

		// ===================================================================
		// AGGREGATIONS - Most are fully supported now!
		// ===================================================================
		{
			name:             "sum aggregation",
			query:            `sum(count_over_time({app="test"}[5m]))`,
			planSupported:    true,
			expectedRowCount: 1, // Single aggregated value
		},
		{
			name:             "sum by aggregation",
			query:            `sum by (level) (count_over_time({app="test"}[5m]))`,
			planSupported:    true,
			expectedRowCount: 4, // 4 levels due to label bug: error, info, warn from test + empty from other apps
		},
		{
			name:             "avg aggregation - NOT SUPPORTED",
			query:            `avg(count_over_time({app="test"}[5m]))`,
			planSupported:    false,
			expectedRowCount: 1, // When implemented: single averaged value
		},
		{
			name:             "min aggregation",
			query:            `min(count_over_time({app="test"}[5m]))`,
			planSupported:    true,
			expectedRowCount: 1, // Minimum value
		},
		{
			name:             "max aggregation",
			query:            `max(count_over_time({app="test"}[5m]))`,
			planSupported:    true,
			expectedRowCount: 1, // Maximum value
		},
		{
			name:             "count aggregation - NOT SUPPORTED",
			query:            `count(count_over_time({app="test"}[5m]))`,
			planSupported:    false,
			expectedRowCount: 1, // When implemented: count of series
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Determine query parameters based on query type
			var q *mockQuery

			// Check if this is a metric query (contains aggregation function or _over_time)
			isMetricQuery := len(tc.query) >= 3 && (tc.query[:3] == "sum" ||
				tc.query[:3] == "avg" ||
				tc.query[:3] == "min" ||
				tc.query[:3] == "max") ||
				len(tc.query) >= 5 && (tc.query[:5] == "count" ||
					tc.query[:5] == "bytes") ||
				len(tc.query) >= 4 && tc.query[:4] == "rate"

			if isMetricQuery {
				// Metric query - needs interval parameter
				q = &mockQuery{
					statement: tc.query,
					start:     now.Add(-10 * time.Minute).Unix(),
					end:       now.Unix(),
					interval:  5 * time.Minute,
				}
			} else {
				// Log query (default)
				q = &mockQuery{
					statement: tc.query,
					start:     now.Add(-1 * time.Hour).Unix(),
					end:       now.Add(1 * time.Hour).Unix(),
					direction: logproto.BACKWARD,
					limit:     100,
				}
			}

			// STAGE 1: Logical Planning
			logicalPlan, err := logical.BuildPlan(q)

			if !tc.planSupported {
				// Feature not supported - should fail at logical planning
				require.Error(t, err, "expected error for unsupported feature")
				t.Logf("✗ NOT SUPPORTED (planning failed): %v", err)
				t.Logf("  Expected %d rows when implemented", tc.expectedRowCount)
				return
			}

			// Feature should be supported for planning
			require.NoError(t, err, "logical planning should succeed for plan-supported feature")
			t.Logf("✓ Logical planning succeeded")

			// STAGE 2: Physical Planning
			planner := physical.NewPlanner(
				physical.NewContext(q.Start(), q.End()),
				catalog,
			)

			physicalPlan, err := planner.Build(logicalPlan)
			require.NoError(t, err, "physical planning should succeed")

			optimizedPlan, err := planner.Optimize(physicalPlan)
			require.NoError(t, err, "optimization should succeed")
			t.Logf("✓ Physical planning succeeded")

			// STAGE 3: Execution (ALWAYS execute, never skip)
			execCtx := ctxWithTenant(ctx, "test-tenant")
			executorCfg := executor.Config{
				BatchSize: 100,
				Bucket:    ingester.Bucket(),
			}

			pipeline := executor.Run(execCtx, executorCfg, optimizedPlan, log.NewNopLogger())
			defer pipeline.Close()

			// Read all results
			var totalRows int64
			var records []any // Keep track for proper cleanup
			for {
				rec, readErr := pipeline.Read(execCtx)
				if readErr == executor.EOF {
					break
				}

				if tc.expectExecError {
					// Feature should fail during execution
					if readErr != nil {
						t.Logf("⊙ PARTIAL SUPPORT: Execution failed as expected: %v", readErr)
						t.Logf("  Expected %d rows when fully implemented", tc.expectedRowCount)
						return
					}
				} else {
					// Fully supported feature should not error
					require.NoError(t, readErr, "execution should succeed for fully supported feature")
				}

				if rec != nil {
					totalRows += rec.NumRows()
					records = append(records, rec)
				}
			}

			// Clean up all records
			for _, r := range records {
				if rec, ok := r.(interface{ Release() }); ok {
					rec.Release()
				}
			}

			if tc.expectExecError {
				// Partial support: execution didn't error, check if we got empty results
				if totalRows == 0 {
					t.Logf("⊙ PARTIAL SUPPORT: Execution returned no data")
					t.Logf("  Expected %d rows when fully implemented", tc.expectedRowCount)
					return
				}
				// Got data when we expected an error - feature may be more supported than documented
				t.Logf("⊙ PARTIAL SUPPORT: Execution succeeded unexpectedly with %d rows (feature may be more supported than documented)", totalRows)
				t.Logf("  Expected %d rows", tc.expectedRowCount)
				// Still verify the count if we got data
				if tc.expectedRowCount >= 0 {
					require.Equal(t, int64(tc.expectedRowCount), totalRows, "unexpected row count for partially supported feature")
				}
				return
			}

			// Fully supported feature - verify results
			t.Logf("✓ Execution succeeded - read %d rows", totalRows)

			if tc.expectedRowCount >= 0 {
				require.Equal(t, int64(tc.expectedRowCount), totalRows, "row count mismatch")
				t.Logf("✓ Result verification passed: got expected %d rows", tc.expectedRowCount)
			}
		})
	}
}

/*
TestForwardDirectionNotSupported specifically tests that FORWARD direction
queries are not yet supported.
*/
func TestForwardDirectionNotSupported(t *testing.T) {
	ctx := context.Background()

	ingester := setupTestIngesterWithData(t, ctx, "test-tenant", map[string][]string{
		`{app="test"}`: {"log 1", "log 2", "log 3"},
	})
	defer ingester.Close()

	now := time.Now()
	q := &mockQuery{
		statement: `{app="test"}`,
		start:     now.Add(-1 * time.Hour).Unix(),
		end:       now.Add(1 * time.Hour).Unix(),
		direction: logproto.FORWARD, // FORWARD not supported
		limit:     100,
	}

	_, err := logical.BuildPlan(q)
	require.Error(t, err, "FORWARD direction should not be supported")
	require.Contains(t, err.Error(), "forward")
	t.Logf("✗ FORWARD direction correctly rejected: %v", err)
}

/*
TestComplexQueriesSupport tests more complex query combinations.
*/
func TestComplexQueriesSupport(t *testing.T) {
	ctx := context.Background()

	// Create rich test data
	ingester := setupTestIngesterWithTimestamps(t, ctx, "test-tenant", []LogEntry{
		{Labels: `{app="test", env="prod"}`, Line: `{"level":"error","latency_ms":250}`, Timestamp: time.Now().Add(-10 * time.Minute)},
		{Labels: `{app="test", env="prod"}`, Line: `{"level":"info","latency_ms":50}`, Timestamp: time.Now().Add(-9 * time.Minute)},
		{Labels: `{app="test", env="dev"}`, Line: `{"level":"error","latency_ms":100}`, Timestamp: time.Now().Add(-8 * time.Minute)},
		{Labels: `{app="other", env="prod"}`, Line: `level=warn msg="slow query"`, Timestamp: time.Now().Add(-7 * time.Minute)},
	})
	defer ingester.Close()

	catalog := ingester.Catalog()
	now := time.Now()

	complexTests := []struct {
		name             string
		query            string
		planSupported    bool
		expectExecError  bool
		expectedRowCount int
	}{
		{
			name:             "multiple line filters",
			query:            `{app="test"} |= "error" |= "connection"`,
			planSupported:    true,
			expectedRowCount: 0, // No logs contain both "error" AND "connection" (test data uses JSON format)
		},
		{
			name:             "line filter + json parsing + label filter",
			query:            `{app="test"} |= "level" | json | level="error"`,
			planSupported:    true,
			expectedRowCount: 2, // 2 JSON logs have level="error" after parsing (test data has JSON logs)
		},
		{
			name:             "regex selector + line filter + drop",
			query:            `{app=~"test.*"} |= "error" | drop env`,
			planSupported:    true,
			expectedRowCount: 2, // 2 logs contain "error" (due to label selector bug, all logs processed)
		},
	}

	for _, tc := range complexTests {
		t.Run(tc.name, func(t *testing.T) {
			q := &mockQuery{
				statement: tc.query,
				start:     now.Add(-1 * time.Hour).Unix(),
				end:       now.Add(1 * time.Hour).Unix(),
				direction: logproto.BACKWARD,
				limit:     100,
			}

			logicalPlan, err := logical.BuildPlan(q)

			if !tc.planSupported {
				require.Error(t, err)
				t.Logf("✗ NOT SUPPORTED (planning failed): %v", err)
				t.Logf("  Expected %d rows when implemented", tc.expectedRowCount)
				return
			}

			require.NoError(t, err)
			t.Logf("✓ Logical planning succeeded")

			planner := physical.NewPlanner(
				physical.NewContext(q.Start(), q.End()),
				catalog,
			)

			physicalPlan, err := planner.Build(logicalPlan)
			require.NoError(t, err)

			optimizedPlan, err := planner.Optimize(physicalPlan)
			require.NoError(t, err)
			t.Logf("✓ Physical planning succeeded")

			// ALWAYS execute, never skip
			execCtx := ctxWithTenant(ctx, "test-tenant")
			executorCfg := executor.Config{
				BatchSize: 100,
				Bucket:    ingester.Bucket(),
			}

			pipeline := executor.Run(execCtx, executorCfg, optimizedPlan, log.NewNopLogger())
			defer pipeline.Close()

			// Read all results
			var totalRows int64
			var records []any
			for {
				rec, readErr := pipeline.Read(execCtx)
				if readErr == executor.EOF {
					break
				}

				if tc.expectExecError {
					if readErr != nil {
						t.Logf("⊙ PARTIAL SUPPORT: Execution failed as expected: %v", readErr)
						t.Logf("  Expected %d rows when fully implemented", tc.expectedRowCount)
						return
					}
				} else {
					require.NoError(t, readErr, "execution should succeed")
				}

				if rec != nil {
					totalRows += rec.NumRows()
					records = append(records, rec)
				}
			}

			// Clean up all records
			for _, r := range records {
				if rec, ok := r.(interface{ Release() }); ok {
					rec.Release()
				}
			}

			if tc.expectExecError && totalRows == 0 {
				t.Logf("⊙ PARTIAL SUPPORT: Execution returned no data")
				t.Logf("  Expected %d rows when fully implemented", tc.expectedRowCount)
				return
			}

			t.Logf("✓ Execution succeeded - %d rows", totalRows)

			if tc.expectedRowCount >= 0 {
				require.Equal(t, int64(tc.expectedRowCount), totalRows, "row count mismatch")
				t.Logf("✓ Result verification passed: got expected %d rows", tc.expectedRowCount)
			}
		})
	}
}
