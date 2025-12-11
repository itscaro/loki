package engine

/*
================================================================================
END-TO-END LEARNING LAB: COMPLETE QUERY EXECUTION FLOW
================================================================================

This file demonstrates the complete journey of a LogQL query through the
Loki Query Engine V2, from initial query string to final results. It ties
together all concepts from the previous stage files into cohesive examples.

================================================================================
THE COMPLETE QUERY PIPELINE
================================================================================

A LogQL query passes through 6 stages:

  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │   LogQL     │    │  Logical    │    │  Physical   │
  │   String    │───▶│   Plan      │───▶│   Plan      │
  │             │    │  (SSA IR)   │    │   (DAG)     │
  └─────────────┘    └─────────────┘    └─────────────┘
                                              │
        Stage 1: Parse          Stage 2:      │    Stage 3:
        & Build SSA             Build DAG     │    Create Tasks
                                              ▼
  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │   LogQL     │    │  Workflow   │    │   Arrow     │
  │   Result    │◀───│  Execution  │◀───│   (Tasks)   │
  │             │    │             │    │             │
  └─────────────┘    └─────────────┘    └─────────────┘
        Stage 6:           Stage 5:          Stage 4:
        Build Result       Run Workflow      Execute Tasks

================================================================================
DATA TRANSFORMATIONS AT EACH STAGE
================================================================================

  Stage 1 - LogQL → SSA:
    Input:  `{app="test"} |= "error"`
    Output: %1 = EQ label.app "test"
            %2 = MATCH_STR builtin.message "error"
            %3 = MAKETABLE [selector=%1, predicates=[%2]]
            ...

  Stage 2 - SSA → DAG:
    Input:  Linear SSA instructions
    Output: DAG with nodes:
            TopK → Filter → ScanSet → [DataObjScan, DataObjScan]

  Stage 3 - DAG → Workflow:
    Input:  Physical plan DAG
    Output: Task graph with streams:
            Task[TopK] ← Stream ← Task[Filter+Scan1]
                       ← Stream ← Task[Filter+Scan2]

  Stage 4 - Tasks → Pipelines:
    Input:  Task fragments
    Output: Pipeline tree producing Arrow RecordBatches

  Stage 5 - Pipelines → Arrow:
    Input:  Connected pipelines
    Output: Stream of Arrow RecordBatches

  Stage 6 - Arrow → LogQL Result:
    Input:  Arrow RecordBatches
    Output: logqlmodel.Streams or promql.Vector/Matrix

================================================================================
*/

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/logical"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/semconv"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
	"github.com/grafana/loki/v3/pkg/engine/internal/util/dag"
	"github.com/grafana/loki/v3/pkg/engine/internal/workflow"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/util/arrowtest"
)

// ============================================================================
// END-TO-END LOG QUERY TESTS
// ============================================================================

/*
TestEndToEnd_SimpleLogQuery demonstrates the complete journey of a simple
log query through all stages of the V2 engine.

This is the most fundamental query pattern - selecting logs from a stream
with a label selector and line filter.
*/
func TestEndToEnd_SimpleLogQuery(t *testing.T) {
	/*
	   ============================================================================
	   QUERY: {app="test"} |= "error"
	   ============================================================================

	   This query:
	   - Selects log streams where app="test"
	   - Filters lines containing "error"
	   - Returns up to 100 results, newest first

	   Expected Result Type: logqlmodel.Streams (log entries grouped by stream)
	*/

	// ============================================================================
	// STAGE 1: LOGICAL PLANNING
	// ============================================================================
	t.Run("stage1_logical_planning", func(t *testing.T) {
		/*
		   Input: LogQL query string + parameters
		   Output: SSA intermediate representation

		   The logical planner converts the parsed AST into SSA form where:
		   - Each variable is assigned exactly once
		   - Data flow is explicit
		   - Operations are independent of physical storage
		*/
		q := &mockQuery{
			statement: `{app="test"} |= "error"`,
			start:     1000,
			end:       2000,
			direction: logproto.BACKWARD,
			limit:     100,
		}

		plan, err := logical.BuildPlan(q)
		require.NoError(t, err)

		t.Logf("\n=== STAGE 1: LOGICAL PLAN (SSA) ===\n%s", plan.String())

		/*
		   Expected SSA output:
		   %1 = EQ label.app "test"              <- Stream selector predicate
		   %2 = MATCH_STR builtin.message "error" <- Line filter predicate
		   %3 = MAKETABLE [selector=%1, predicates=[%2], shard=0_of_1]
		   %4 = GTE builtin.timestamp <start>
		   %5 = SELECT %3 [predicate=%4]
		   %6 = LT builtin.timestamp <end>
		   %7 = SELECT %5 [predicate=%6]
		   %8 = SELECT %7 [predicate=%2]         <- Apply line filter
		   %9 = TOPK %8 [sort_by=builtin.timestamp, k=100, asc=false]
		   %10 = LOGQL_COMPAT %9
		   RETURN %10

		   Key observations:
		   1. Predicates are created before use (functional style)
		   2. MAKETABLE defines the data source with pushdown hints
		   3. SELECT operations apply filters step by step
		   4. TOPK handles both sorting and limiting
		   5. LOGQL_COMPAT ensures output format compatibility
		*/

		planStr := plan.String()
		require.Contains(t, planStr, "EQ label.app")
		require.Contains(t, planStr, "MATCH_STR builtin.message")
		require.Contains(t, planStr, "MAKETABLE")
		require.Contains(t, planStr, "SELECT")
		require.Contains(t, planStr, "TOPK")
	})

	// ============================================================================
	// STAGE 2: PHYSICAL PLANNING
	// ============================================================================
	t.Run("stage2_physical_planning", func(t *testing.T) {
		/*
		   Input: Logical plan (SSA)
		   Output: Physical plan (DAG) with concrete data sources

		   The physical planner:
		   - Resolves MAKETABLE to actual data object locations via catalog
		   - Converts SSA instructions to DAG nodes
		   - Applies optimization passes (pushdown, parallelization)
		*/
		q := &mockQuery{
			statement: `{app="test"} |= "error"`,
			start:     1000,
			end:       2000,
			direction: logproto.BACKWARD,
			limit:     100,
		}

		logicalPlan, err := logical.BuildPlan(q)
		require.NoError(t, err)

		// Create catalog with test data objects
		now := time.Now()
		catalog := &mockCatalog{
			sectionDescriptors: []*metastore.DataobjSectionDescriptor{
				{
					SectionKey: metastore.SectionKey{ObjectPath: "tenant/obj1", SectionIdx: 0},
					StreamIDs:  []int64{1, 2},
					Start:      now,
					End:        now.Add(time.Hour),
				},
				{
					SectionKey: metastore.SectionKey{ObjectPath: "tenant/obj2", SectionIdx: 0},
					StreamIDs:  []int64{3, 4},
					Start:      now,
					End:        now.Add(time.Hour),
				},
			},
		}

		planner := physical.NewPlanner(
			physical.NewContext(q.Start(), q.End()),
			catalog,
		)

		physicalPlan, err := planner.Build(logicalPlan)
		require.NoError(t, err)

		t.Logf("\n=== STAGE 2: PHYSICAL PLAN (Before Optimization) ===\n%s",
			physical.PrintAsTree(physicalPlan))

		// Apply optimizations
		optimizedPlan, err := planner.Optimize(physicalPlan)
		require.NoError(t, err)

		t.Logf("\n=== STAGE 2: PHYSICAL PLAN (After Optimization) ===\n%s",
			physical.PrintAsTree(optimizedPlan))

		/*
		   Expected physical plan structure (after optimization):
		   TopK
		   └── Parallelize
		         └── ScanSet [predicates pushed down]
		               ├── DataObjScan [tenant/obj1]
		               └── DataObjScan [tenant/obj2]

		   Key observations:
		   1. MAKETABLE resolved to ScanSet with multiple scan targets
		   2. Predicates pushed down to ScanSet (optimization)
		   3. Parallelize node inserted for distributed execution
		   4. Filter nodes may be eliminated after pushdown
		*/

		root, err := optimizedPlan.Root()
		require.NoError(t, err)
		require.NotNil(t, root)
	})

	// ============================================================================
	// STAGE 3: WORKFLOW PLANNING
	// ============================================================================
	t.Run("stage3_workflow_planning", func(t *testing.T) {
		/*
		   Input: Physical plan (DAG)
		   Output: Workflow with tasks and streams

		   The workflow planner:
		   - Partitions the DAG at pipeline breakers
		   - Creates tasks for parallel execution
		   - Connects tasks via data streams
		*/

		// Build a simple physical plan manually for demonstration
		var graph dag.Graph[physical.Node]

		scanSet := graph.Add(&physical.ScanSet{
			Targets: []*physical.ScanTarget{
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj1"}},
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj2"}},
			},
		})
		parallelize := graph.Add(&physical.Parallelize{})
		topK := graph.Add(&physical.TopK{
			K:         100,
			Ascending: false,
			SortBy: &physical.ColumnExpr{
				Ref: types.ColumnRef{
					Column: "timestamp",
					Type:   types.ColumnTypeBuiltin,
				},
			},
		})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: topK, Child: parallelize})

		physicalPlan := physical.FromGraph(graph)

		runner := newTestRunner()
		wf, err := workflow.New(
			workflow.Options{},
			log.NewNopLogger(),
			"test-tenant",
			runner,
			physicalPlan,
		)
		require.NoError(t, err)

		t.Logf("\n=== STAGE 3: WORKFLOW ===\n%s", workflow.Sprint(wf))

		/*
		   Expected workflow structure:
		   Workflow:
		     Tasks:
		       - Task[0]: TopK (root task, receives merged results)
		       - Task[1]: Scan obj1 (leaf task)
		       - Task[2]: Scan obj2 (leaf task)
		     Streams:
		       - Stream[0]: Task[1] → Task[0]
		       - Stream[1]: Task[2] → Task[0]

		   Key observations:
		   1. Pipeline breakers (TopK) force task boundaries
		   2. Each scan target becomes a separate task
		   3. Streams connect tasks for data flow
		   4. Root task aggregates results from leaf tasks
		*/
	})

	// ============================================================================
	// STAGES 4-5: EXECUTION (Simulated)
	// ============================================================================
	t.Run("stage4_5_execution", func(t *testing.T) {
		/*
		   Input: Workflow tasks
		   Output: Arrow RecordBatches

		   Execution involves:
		   1. Scheduler assigns tasks to workers
		   2. Workers execute task fragments
		   3. Pipelines read from storage and produce Arrow batches
		   4. Results flow through streams to root
		*/

		// Simulate execution by creating mock Arrow data
		alloc := memory.NewGoAllocator()

		colTs := semconv.ColumnIdentTimestamp
		colMsg := semconv.ColumnIdentMessage
		colApp := semconv.NewIdentifier("app", types.ColumnTypeLabel, types.Loki.String)

		schema := arrow.NewSchema(
			[]arrow.Field{
				semconv.FieldFromIdent(colTs, false),
				semconv.FieldFromIdent(colMsg, false),
				semconv.FieldFromIdent(colApp, false),
			},
			nil,
		)

		// Simulated results from Scan Task 1
		rows1 := arrowtest.Rows{
			{colTs.FQN(): time.Unix(0, 1000000003).UTC(), colMsg.FQN(): "error: connection failed", colApp.FQN(): "test"},
			{colTs.FQN(): time.Unix(0, 1000000001).UTC(), colMsg.FQN(): "error: timeout occurred", colApp.FQN(): "test"},
		}
		record1 := rows1.Record(alloc, schema)
		defer record1.Release()

		// Simulated results from Scan Task 2
		rows2 := arrowtest.Rows{
			{colTs.FQN(): time.Unix(0, 1000000002).UTC(), colMsg.FQN(): "error: invalid input", colApp.FQN(): "test"},
		}
		record2 := rows2.Record(alloc, schema)
		defer record2.Release()

		t.Logf("\n=== STAGES 4-5: ARROW RECORDBATCHES ===")
		t.Logf("Record 1 from Scan Task 1: %d rows", record1.NumRows())
		t.Logf("Record 2 from Scan Task 2: %d rows", record2.NumRows())

		// Verify Arrow data structure
		require.Equal(t, int64(2), record1.NumRows())
		require.Equal(t, int64(1), record2.NumRows())

		/*
		   Expected flow:
		   1. Scan Task 1 reads obj1, produces record1 (2 rows)
		   2. Scan Task 2 reads obj2, produces record2 (1 row)
		   3. TopK Task receives both records via streams
		   4. TopK merges and sorts by timestamp descending
		   5. TopK outputs final sorted record (3 rows, limited to k=100)
		*/
	})

	// ============================================================================
	// STAGE 6: RESULT BUILDING
	// ============================================================================
	t.Run("stage6_result_building", func(t *testing.T) {
		/*
		   Input: Arrow RecordBatches
		   Output: logqlmodel.Streams

		   The result builder:
		   - Reads columns from Arrow batches
		   - Groups entries by stream labels
		   - Creates logproto.Entry objects with timestamp and line
		   - Returns sorted streams
		*/

		// Create final merged/sorted Arrow result
		alloc := memory.NewGoAllocator()

		colTs := semconv.ColumnIdentTimestamp
		colMsg := semconv.ColumnIdentMessage
		colApp := semconv.NewIdentifier("app", types.ColumnTypeLabel, types.Loki.String)

		schema := arrow.NewSchema(
			[]arrow.Field{
				semconv.FieldFromIdent(colTs, false),
				semconv.FieldFromIdent(colMsg, false),
				semconv.FieldFromIdent(colApp, false),
			},
			nil,
		)

		// Final sorted results (newest first for BACKWARD direction)
		finalRows := arrowtest.Rows{
			{colTs.FQN(): time.Unix(0, 1000000003).UTC(), colMsg.FQN(): "error: connection failed", colApp.FQN(): "test"},
			{colTs.FQN(): time.Unix(0, 1000000002).UTC(), colMsg.FQN(): "error: invalid input", colApp.FQN(): "test"},
			{colTs.FQN(): time.Unix(0, 1000000001).UTC(), colMsg.FQN(): "error: timeout occurred", colApp.FQN(): "test"},
		}
		finalRecord := finalRows.Record(alloc, schema)
		defer finalRecord.Release()

		// Simulate result building (extract data from Arrow)
		t.Logf("\n=== STAGE 6: RESULT BUILDING ===")

		// Read timestamp column
		tsCol := finalRecord.Column(0).(*array.Timestamp)
		// Read message column
		msgCol := finalRecord.Column(1).(*array.String)
		// Read label column
		appCol := finalRecord.Column(2).(*array.String)

		t.Logf("Final Results (logqlmodel.Streams):")
		t.Logf("Stream: {app=\"%s\"}", appCol.Value(0))
		for i := 0; i < int(finalRecord.NumRows()); i++ {
			ts := time.Unix(0, tsCol.Value(i).ToTime(arrow.Nanosecond).UnixNano())
			msg := msgCol.Value(i)
			t.Logf("  [%v] %s", ts, msg)
		}

		/*
		   Expected LogQL Result:
		   logqlmodel.Streams{
		       {
		           Labels: {app="test"},
		           Entries: [
		               {Timestamp: ..., Line: "error: connection failed"},
		               {Timestamp: ..., Line: "error: invalid input"},
		               {Timestamp: ..., Line: "error: timeout occurred"},
		           ]
		       }
		   }

		   Key observations:
		   1. Entries are grouped by unique label combinations
		   2. Entries within each stream are sorted by timestamp
		   3. Direction (BACKWARD) determines sort order
		   4. Result is compatible with V1 LogQL API
		*/

		require.Equal(t, int64(3), finalRecord.NumRows())
		require.Equal(t, "error: connection failed", msgCol.Value(0)) // Newest
		require.Equal(t, "error: timeout occurred", msgCol.Value(2))  // Oldest
	})
}

// ============================================================================
// END-TO-END METRIC QUERY TESTS
// ============================================================================

/*
TestEndToEnd_MetricQuery demonstrates the complete journey of a metric
query through all stages.

Metric queries differ from log queries:
- They return numeric time series instead of log lines
- They involve range and vector aggregations
- The result type is promql.Vector or promql.Matrix
*/
func TestEndToEnd_MetricQuery(t *testing.T) {
	/*
	   ============================================================================
	   QUERY: sum by (level) (count_over_time({app="test"}[5m]))
	   ============================================================================

	   This query:
	   - Counts log lines per 5-minute window for each stream
	   - Groups and sums the counts by the "level" label
	   - Returns a vector (single point per series) or matrix (time series)

	   Expected Result Type: promql.Vector (for instant queries)
	                        promql.Matrix (for range queries)
	*/

	// ============================================================================
	// STAGE 1: LOGICAL PLANNING FOR METRIC QUERIES
	// ============================================================================
	t.Run("stage1_metric_logical_plan", func(t *testing.T) {
		q := &mockQuery{
			statement: `sum by (level) (count_over_time({app="test"}[5m]))`,
			start:     3600,
			end:       7200,
			interval:  5 * time.Minute,
		}

		plan, err := logical.BuildPlan(q)
		require.NoError(t, err)

		t.Logf("\n=== METRIC QUERY LOGICAL PLAN ===\n%s", plan.String())

		/*
		   Expected SSA output:
		   %1 = EQ label.app "test"
		   %2 = MAKETABLE [selector=%1, predicates=[], shard=0_of_1]
		   %3 = GTE builtin.timestamp <start - 5min>  <- Lookback adjustment!
		   %4 = SELECT %2 [predicate=%3]
		   %5 = LT builtin.timestamp <end>
		   %6 = SELECT %4 [predicate=%5]
		   %7 = RANGE_AGGREGATION %6 [operation=count, range=5m]
		   %8 = VECTOR_AGGREGATION %7 [operation=sum, group_by=(level)]
		   %9 = LOGQL_COMPAT %8
		   RETURN %9

		   Key differences from log queries:
		   1. No TOPK - results are aggregated values, not sorted logs
		   2. Time range starts earlier (lookback for range aggregation)
		   3. RANGE_AGGREGATION counts entries per time window
		   4. VECTOR_AGGREGATION groups and sums across series
		*/

		planStr := plan.String()
		require.Contains(t, planStr, "RANGE_AGGREGATION")
		require.Contains(t, planStr, "VECTOR_AGGREGATION")
		require.Contains(t, planStr, "operation=count")
		require.Contains(t, planStr, "operation=sum")
	})

	// ============================================================================
	// METRIC QUERY EXECUTION FLOW
	// ============================================================================
	t.Run("metric_query_execution_flow", func(t *testing.T) {
		/*
		   Metric query execution differs from log queries:

		   1. Scan tasks read log entries (same as log queries)
		   2. RangeAggregation task counts entries per time window:
		      - Groups by stream labels + timestamp bucket
		      - Produces count values per window
		   3. VectorAggregation task sums counts:
		      - Groups by specified labels (level)
		      - Produces final aggregated values

		   Arrow data transformation:
		   - Scan output: (timestamp, message, labels)
		   - RangeAgg output: (timestamp, value, labels) where value = count
		   - VectorAgg output: (timestamp, value, grouped_labels) where value = sum
		*/

		// Simulate RangeAggregation output
		alloc := memory.NewGoAllocator()

		colTs := semconv.ColumnIdentTimestamp
		colVal := semconv.ColumnIdentValue
		colLevel := semconv.NewIdentifier("level", types.ColumnTypeLabel, types.Loki.String)

		rangeAggSchema := arrow.NewSchema(
			[]arrow.Field{
				semconv.FieldFromIdent(colTs, false),
				semconv.FieldFromIdent(colVal, false),
				semconv.FieldFromIdent(colLevel, false),
			},
			nil,
		)

		// After RangeAggregation: count_over_time results
		rangeAggRows := arrowtest.Rows{
			// Stream 1: {level="error"} - 10 entries in window
			{colTs.FQN(): time.Unix(3600, 0).UTC(), colVal.FQN(): float64(10), colLevel.FQN(): "error"},
			// Stream 2: {level="error"} - 5 entries in window
			{colTs.FQN(): time.Unix(3600, 0).UTC(), colVal.FQN(): float64(5), colLevel.FQN(): "error"},
			// Stream 3: {level="info"} - 100 entries in window
			{colTs.FQN(): time.Unix(3600, 0).UTC(), colVal.FQN(): float64(100), colLevel.FQN(): "info"},
		}
		rangeAggRecord := rangeAggRows.Record(alloc, rangeAggSchema)
		defer rangeAggRecord.Release()

		t.Logf("\n=== AFTER RANGE_AGGREGATION ===")
		t.Logf("Rows: %d (one per stream per time window)", rangeAggRecord.NumRows())

		// After VectorAggregation: sum by (level)
		// This would merge streams with same level label
		vectorAggRows := arrowtest.Rows{
			// {level="error"}: 10 + 5 = 15
			{colTs.FQN(): time.Unix(3600, 0).UTC(), colVal.FQN(): float64(15), colLevel.FQN(): "error"},
			// {level="info"}: 100
			{colTs.FQN(): time.Unix(3600, 0).UTC(), colVal.FQN(): float64(100), colLevel.FQN(): "info"},
		}
		vectorAggRecord := vectorAggRows.Record(alloc, rangeAggSchema)
		defer vectorAggRecord.Release()

		t.Logf("\n=== AFTER VECTOR_AGGREGATION ===")
		t.Logf("Rows: %d (one per unique label group)", vectorAggRecord.NumRows())

		// Read final values
		valCol := vectorAggRecord.Column(1).(*array.Float64)
		levelCol := vectorAggRecord.Column(2).(*array.String)

		t.Logf("\nFinal Metric Results (promql.Vector):")
		for i := 0; i < int(vectorAggRecord.NumRows()); i++ {
			t.Logf("  {level=\"%s\"} = %v", levelCol.Value(i), valCol.Value(i))
		}

		/*
		   Expected Result:
		   promql.Vector{
		       {Metric: {level="error"}, Point: {T: 3600000, V: 15}},
		       {Metric: {level="info"}, Point: {T: 3600000, V: 100}},
		   }

		   Key observations:
		   1. Multiple streams merged into fewer series
		   2. Values are aggregated (sum in this case)
		   3. Labels reduced to only group_by labels
		   4. Timestamp represents the evaluation point
		*/

		require.Equal(t, int64(2), vectorAggRecord.NumRows())
		require.Equal(t, float64(15), valCol.Value(0))  // error: 10+5
		require.Equal(t, float64(100), valCol.Value(1)) // info: 100
	})
}

// ============================================================================
// END-TO-END WITH JSON PARSING
// ============================================================================

/*
TestEndToEnd_JSONParsingQuery demonstrates a query that parses JSON
from log lines and filters on extracted fields.
*/
func TestEndToEnd_JSONParsingQuery(t *testing.T) {
	/*
	   ============================================================================
	   QUERY: {app="test"} | json | level="error"
	   ============================================================================

	   This query:
	   - Selects log streams where app="test"
	   - Parses each log line as JSON
	   - Filters entries where the parsed "level" field equals "error"

	   Pipeline stages:
	   1. Stream selection: {app="test"}
	   2. JSON parsing: | json
	   3. Field filtering: | level="error"
	*/

	t.Run("json_parsing_flow", func(t *testing.T) {
		// STAGE 1: Logical planning
		q := &mockQuery{
			statement: `{app="test"} | json | level="error"`,
			start:     1000,
			end:       2000,
			direction: logproto.BACKWARD,
			limit:     100,
		}

		plan, err := logical.BuildPlan(q)
		require.NoError(t, err)

		t.Logf("\n=== JSON QUERY LOGICAL PLAN ===\n%s", plan.String())

		planStr := plan.String()
		require.Contains(t, planStr, "PROJECT")
		require.Contains(t, planStr, "PARSE_JSON")
		require.Contains(t, planStr, "ambiguous.level")

		/*
		   SSA highlights for JSON parsing:

		   %N = PROJECT %input [mode=*E, expr=PARSE_JSON(builtin.message, [], false, false)]

		   This PROJECT instruction:
		   - mode=*E: Extend mode - adds new columns while keeping existing
		   - PARSE_JSON: Parses the message column as JSON
		   - []: Extract all keys (empty list means all)
		   - false, false: Not strict mode, don't keep as strings

		   The filter uses "ambiguous.level" because at logical planning
		   time, we don't know if level is a label or parsed field.
		   Physical planning resolves this based on schema.
		*/

		// Simulate Arrow data after JSON parsing
		alloc := memory.NewGoAllocator()

		colTs := semconv.ColumnIdentTimestamp
		colMsg := semconv.ColumnIdentMessage
		colApp := semconv.NewIdentifier("app", types.ColumnTypeLabel, types.Loki.String)
		colLevel := semconv.NewIdentifier("level", types.ColumnTypeParsed, types.Loki.String) // Parsed!

		schema := arrow.NewSchema(
			[]arrow.Field{
				semconv.FieldFromIdent(colTs, false),
				semconv.FieldFromIdent(colMsg, false),
				semconv.FieldFromIdent(colApp, false),
				semconv.FieldFromIdent(colLevel, false), // Added by JSON parsing
			},
			nil,
		)

		// After JSON parsing and filtering
		rows := arrowtest.Rows{
			{
				colTs.FQN():    time.Unix(0, 1000000002).UTC(),
				colMsg.FQN():   `{"level": "error", "msg": "connection failed"}`,
				colApp.FQN():   "test",
				colLevel.FQN(): "error",
			},
			{
				colTs.FQN():    time.Unix(0, 1000000001).UTC(),
				colMsg.FQN():   `{"level": "error", "msg": "timeout"}`,
				colApp.FQN():   "test",
				colLevel.FQN(): "error",
			},
		}
		record := rows.Record(alloc, schema)
		defer record.Release()

		t.Logf("\n=== AFTER JSON PARSING ===")
		t.Logf("Columns: %d (original + parsed)", record.NumCols())
		for i, field := range record.Schema().Fields() {
			t.Logf("  Column %d: %s", i, field.Name)
		}

		/*
		   Arrow schema after JSON parsing:
		   - timestamp_ns.builtin.timestamp (original)
		   - utf8.builtin.message (original)
		   - utf8.label.app (original)
		   - utf8.parsed.level (NEW - from JSON)

		   Note the "parsed." prefix indicating this column was
		   extracted during query execution, not from stream labels.
		*/

		require.Equal(t, int64(2), record.NumRows())
		require.Equal(t, 4, int(record.NumCols()))
	})
}

// ============================================================================
// TRACING A QUERY THROUGH ALL STAGES
// ============================================================================

/*
TestEndToEnd_TraceQueryFlow provides a comprehensive trace of a query
through all stages, with detailed logging at each step.
*/
func TestEndToEnd_TraceQueryFlow(t *testing.T) {
	t.Log("\n" + `
================================================================================
TRACING QUERY: {app="test"} |= "error" | json | level="error"
================================================================================

This test traces the complete journey of a complex query through all stages.
`)

	query := `{app="test"} |= "error" | json | level="error"`

	// -------------------------------------------------------------------------
	// TRACE POINT 1: Query Parsing
	// -------------------------------------------------------------------------
	t.Log("\n=== TRACE POINT 1: QUERY PARSING ===")
	t.Logf("Input: %s", query)
	t.Log("Output: syntax.Expr (Abstract Syntax Tree)")
	t.Log(`
    LogSelectorExpr
    ├── Matchers: {app="test"}
    └── Pipeline:
        ├── LineFilter: |= "error"
        ├── JSONParser: | json
        └── LabelFilter: | level="error"
`)

	// -------------------------------------------------------------------------
	// TRACE POINT 2: Logical Planning
	// -------------------------------------------------------------------------
	t.Log("\n=== TRACE POINT 2: LOGICAL PLANNING ===")
	q := &mockQuery{
		statement: query,
		start:     1000,
		end:       2000,
		direction: logproto.BACKWARD,
		limit:     100,
	}

	logicalPlan, err := logical.BuildPlan(q)
	require.NoError(t, err)

	t.Log("Output: logical.Plan (SSA Intermediate Representation)")
	t.Logf("\n%s", logicalPlan.String())

	// -------------------------------------------------------------------------
	// TRACE POINT 3: Physical Planning
	// -------------------------------------------------------------------------
	t.Log("\n=== TRACE POINT 3: PHYSICAL PLANNING ===")

	now := time.Now()
	catalog := &mockCatalog{
		sectionDescriptors: []*metastore.DataobjSectionDescriptor{
			{
				SectionKey: metastore.SectionKey{ObjectPath: "tenant/obj1", SectionIdx: 0},
				StreamIDs:  []int64{1, 2},
				Start:      now,
				End:        now.Add(time.Hour),
			},
		},
	}

	planner := physical.NewPlanner(
		physical.NewContext(q.Start(), q.End()),
		catalog,
	)

	physicalPlan, err := planner.Build(logicalPlan)
	require.NoError(t, err)

	optimizedPlan, err := planner.Optimize(physicalPlan)
	require.NoError(t, err)

	t.Log("Output: physical.Plan (DAG of executable nodes)")
	t.Logf("\n%s", physical.PrintAsTree(optimizedPlan))

	// -------------------------------------------------------------------------
	// TRACE POINT 4: Workflow Planning
	// -------------------------------------------------------------------------
	t.Log("\n=== TRACE POINT 4: WORKFLOW PLANNING ===")

	runner := newTestRunner()
	wf, err := workflow.New(
		workflow.Options{},
		log.NewNopLogger(),
		"test-tenant",
		runner,
		optimizedPlan,
	)
	require.NoError(t, err)

	t.Log("Output: workflow.Workflow (Task Graph)")
	t.Logf("\n%s", workflow.Sprint(wf))

	// -------------------------------------------------------------------------
	// TRACE POINT 5: Execution (Simulated)
	// -------------------------------------------------------------------------
	t.Log("\n=== TRACE POINT 5: EXECUTION (Simulated) ===")
	t.Log("Output: Arrow RecordBatches")
	t.Log(`
    Tasks registered: scan tasks + aggregation task
    Streams created: data channels between tasks
    Execution: Workers fetch tasks, execute, send results via streams
`)

	// -------------------------------------------------------------------------
	// TRACE POINT 6: Result Building (Simulated)
	// -------------------------------------------------------------------------
	t.Log("\n=== TRACE POINT 6: RESULT BUILDING (Simulated) ===")
	t.Log("Output: logqlmodel.Streams")
	t.Log(`
    Arrow RecordBatches → logqlmodel.Streams:
    1. Read label columns to identify unique streams
    2. Group entries by stream labels
    3. Sort entries by timestamp per stream
    4. Create logproto.Entry for each row
    5. Return streams in API-compatible format
`)

	t.Log("\n=== QUERY EXECUTION COMPLETE ===")
}

// ============================================================================
// QUERY TYPE COMPARISON
// ============================================================================

/*
TestEndToEnd_QueryTypeComparison compares the pipeline differences between
log queries and metric queries side by side.
*/
func TestEndToEnd_QueryTypeComparison(t *testing.T) {
	t.Log("\n" + `
================================================================================
QUERY TYPE COMPARISON: LOG vs METRIC
================================================================================
`)

	// -------------------------------------------------------------------------
	// LOG QUERY
	// -------------------------------------------------------------------------
	t.Run("log_query", func(t *testing.T) {
		t.Log("LOG QUERY: {app=\"test\"} |= \"error\"")

		q := &mockQuery{
			statement: `{app="test"} |= "error"`,
			start:     1000,
			end:       2000,
			direction: logproto.BACKWARD,
			limit:     100,
		}

		plan, err := logical.BuildPlan(q)
		require.NoError(t, err)

		t.Log("\nCharacteristics:")
		t.Log("  - Returns: logqlmodel.Streams (log entries)")
		t.Log("  - Has TOPK: Yes (sorts by timestamp)")
		t.Log("  - Has Aggregations: No")
		t.Log("  - Time Range: Exact query range")
		t.Log("  - Output Schema: (timestamp, message, labels)")

		planStr := plan.String()
		require.Contains(t, planStr, "TOPK")
		require.NotContains(t, planStr, "RANGE_AGGREGATION")
	})

	// -------------------------------------------------------------------------
	// METRIC QUERY
	// -------------------------------------------------------------------------
	t.Run("metric_query", func(t *testing.T) {
		t.Log("\nMETRIC QUERY: sum by (level) (count_over_time({app=\"test\"}[5m]))")

		q := &mockQuery{
			statement: `sum by (level) (count_over_time({app="test"}[5m]))`,
			start:     3600,
			end:       7200,
			interval:  5 * time.Minute,
		}

		plan, err := logical.BuildPlan(q)
		require.NoError(t, err)

		t.Log("\nCharacteristics:")
		t.Log("  - Returns: promql.Vector/Matrix (numeric series)")
		t.Log("  - Has TOPK: No")
		t.Log("  - Has Aggregations: Yes (RANGE_AGGREGATION, VECTOR_AGGREGATION)")
		t.Log("  - Time Range: Extended by lookback (start - 5m)")
		t.Log("  - Output Schema: (timestamp, value, grouped_labels)")

		planStr := plan.String()
		require.NotContains(t, planStr, "TOPK")
		require.Contains(t, planStr, "RANGE_AGGREGATION")
		require.Contains(t, planStr, "VECTOR_AGGREGATION")
	})

	// -------------------------------------------------------------------------
	// COMPARISON TABLE
	// -------------------------------------------------------------------------
	t.Log("\n" + `
================================================================================
COMPARISON TABLE
================================================================================

| Aspect              | Log Query            | Metric Query           |
|---------------------|---------------------|------------------------|
| Result Type         | Streams             | Vector/Matrix          |
| Output              | Log entries         | Numeric values         |
| Sorting             | TOPK by timestamp   | N/A                    |
| Aggregation         | None                | Range + Vector         |
| Time Adjustment     | Exact range         | Range + lookback       |
| Limit               | Yes (k parameter)   | No                     |
| Direction           | FORWARD/BACKWARD    | N/A                    |
| Group By            | N/A                 | Label grouping         |

================================================================================
`)
}

// ============================================================================
// PIPELINE BREAKERS AND TASK BOUNDARIES
// ============================================================================

/*
TestEndToEnd_PipelineBreakers demonstrates how pipeline breakers affect
workflow task partitioning.
*/
func TestEndToEnd_PipelineBreakers(t *testing.T) {
	t.Log("\n" + `
================================================================================
PIPELINE BREAKERS AND TASK BOUNDARIES
================================================================================

Pipeline breakers are operators that must see ALL input data before producing
output. They force task boundaries in the workflow.

Examples of pipeline breakers:
- TopK: Must see all data to determine top K items
- RangeAggregation: Must see all data in time window
- VectorAggregation: Must see all data for grouping
- Sort: Must see all data to sort

Non-breakers (can be pipelined):
- Filter: Process row by row
- Projection: Transform row by row
- JSON parsing: Parse row by row

================================================================================
`)

	t.Run("topk_creates_task_boundary", func(t *testing.T) {
		/*
		   Physical Plan:
		     TopK                     <- Pipeline breaker
		       └── Parallelize
		             └── ScanSet
		                   ├── Scan1  <- Can run in parallel
		                   └── Scan2  <- Can run in parallel

		   Workflow:
		     Task 1: TopK (receives merged results)
		     Task 2: Scan1 (sends to Task 1)
		     Task 3: Scan2 (sends to Task 1)
		*/

		var graph dag.Graph[physical.Node]

		scanSet := graph.Add(&physical.ScanSet{
			Targets: []*physical.ScanTarget{
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj1"}},
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj2"}},
			},
		})
		parallelize := graph.Add(&physical.Parallelize{})
		topK := graph.Add(&physical.TopK{
			K:         100,
			Ascending: false,
			SortBy: &physical.ColumnExpr{
				Ref: types.ColumnRef{
					Column: "timestamp",
					Type:   types.ColumnTypeBuiltin,
				},
			},
		})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: topK, Child: parallelize})

		plan := physical.FromGraph(graph)

		runner := newTestRunner()
		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, plan)
		require.NoError(t, err)

		t.Logf("Physical Plan:\n%s", physical.PrintAsTree(plan))
		t.Logf("Workflow:\n%s", workflow.Sprint(wf))
	})

	t.Run("multiple_breakers_create_multiple_boundaries", func(t *testing.T) {
		/*
		   Physical Plan:
		     VectorAggregation        <- Pipeline breaker #2
		       └── RangeAggregation   <- Pipeline breaker #1
		             └── Parallelize
		                   └── ScanSet

		   Workflow:
		     Task 1: VectorAggregation (root)
		     Task 2: RangeAggregation (receives from scans, sends to Task 1)
		     Task 3+: Scan tasks (send to Task 2)
		*/

		var graph dag.Graph[physical.Node]

		scanSet := graph.Add(&physical.ScanSet{
			Targets: []*physical.ScanTarget{
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj1"}},
			},
		})
		parallelize := graph.Add(&physical.Parallelize{})
		rangeAgg := graph.Add(&physical.RangeAggregation{
			Operation: types.RangeAggregationTypeCount,
			Range:     5 * time.Minute,
		})
		vectorAgg := graph.Add(&physical.VectorAggregation{
			Operation: types.VectorAggregationTypeSum,
		})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: rangeAgg, Child: parallelize})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: vectorAgg, Child: rangeAgg})

		plan := physical.FromGraph(graph)

		runner := newTestRunner()
		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, plan)
		require.NoError(t, err)

		t.Logf("Physical Plan:\n%s", physical.PrintAsTree(plan))
		t.Logf("Workflow:\n%s", workflow.Sprint(wf))

		// Verify multiple task boundaries were created
		runner.mu.RLock()
		numTasks := len(runner.tasks)
		numStreams := len(runner.streams)
		runner.mu.RUnlock()

		t.Logf("Tasks created: %d", numTasks)
		t.Logf("Streams created: %d", numStreams)
	})
}

// ============================================================================
// SUMMARY: COMPLETE PIPELINE OVERVIEW
// ============================================================================

/*
TestEndToEnd_PipelineSummary provides a final summary of the complete
V2 engine pipeline.
*/
func TestEndToEnd_PipelineSummary(t *testing.T) {
	fmt.Print(`
================================================================================
LOKI QUERY ENGINE V2 - COMPLETE PIPELINE SUMMARY
================================================================================

1. PARSING (not covered - handled by logql/syntax)
   Input:  LogQL string
   Output: syntax.Expr (AST)
   Action: Parse query string into Abstract Syntax Tree

2. LOGICAL PLANNING (stage1_logical_planning_test.go)
   Input:  syntax.Expr + query parameters
   Output: logical.Plan (SSA IR)
   Action: Convert AST to Static Single Assignment form
   Key:    Each variable assigned once, data flow explicit

3. PHYSICAL PLANNING (stage2_physical_planning_test.go)
   Input:  logical.Plan + Catalog
   Output: physical.Plan (DAG)
   Action: Resolve data sources, build executable DAG
   Key:    MAKETABLE → DataObjScan, apply optimizations

4. WORKFLOW PLANNING (stage3_workflow_planning_test.go)
   Input:  physical.Plan
   Output: workflow.Workflow (Task Graph)
   Action: Partition plan at pipeline breakers, create tasks
   Key:    Tasks connected by streams for distributed execution

5. EXECUTION (stage4_execution_test.go, stage5_distributed_execution_test.go)
   Input:  Tasks assigned by scheduler
   Output: Arrow RecordBatches
   Action: Workers execute task fragments, produce results
   Key:    Pipelines form tree structure, data flows up

6. RESULT BUILDING (stage4_execution_test.go)
   Input:  Arrow RecordBatches
   Output: logqlmodel.Streams or promql.Vector/Matrix
   Action: Convert Arrow data to LogQL-compatible results
   Key:    Group by labels, sort by timestamp, format output

================================================================================
DATA FORMATS THROUGH THE PIPELINE
================================================================================

Stage          | Data Format
---------------|--------------------------------------------------
1. Parsing     | LogQL string → syntax.Expr (AST nodes)
2. Logical     | syntax.Expr → logical.Plan (SSA instructions)
3. Physical    | logical.Plan → physical.Plan (DAG nodes)
4. Workflow    | physical.Plan → Tasks + Streams
5. Execution   | Tasks → Arrow RecordBatches (columnar)
6. Result      | Arrow → Streams/Vector/Matrix (API response)

================================================================================
KEY DESIGN PRINCIPLES
================================================================================

1. COLUMNAR PROCESSING
   - Arrow RecordBatches enable vectorized operations
   - Better CPU cache utilization than row-by-row

2. DISTRIBUTED EXECUTION
   - Workflow partitions plan into tasks
   - Tasks execute in parallel across workers
   - Streams connect tasks for data flow

3. OPTIMIZATION OPPORTUNITIES
   - Predicate pushdown to storage
   - Limit pushdown to scans
   - Projection pushdown (read only needed columns)
   - Parallel scan execution

4. TYPE SAFETY
   - SSA form makes data flow explicit
   - Column type prefixes track data origin
   - Physical planning resolves ambiguous types

================================================================================
`)
}
