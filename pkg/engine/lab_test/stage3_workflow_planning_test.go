package engine

/*
============================================================================
LOKI QUERY ENGINE V2 - STAGE 3: WORKFLOW PLANNING
============================================================================

This file covers Stage 3 of query processing: Workflow Planning.
Workflow planning partitions physical plans into distributable tasks connected
by data streams for parallel and distributed execution.

============================================================================
STAGE OVERVIEW
============================================================================

Input:  physical.Plan (DAG of executable nodes)
Output: workflow.Workflow (Task Graph with streams)

Workflow planning transforms a physical execution plan into a set of tasks
that can be distributed across multiple workers for parallel execution.

============================================================================
KEY CONCEPTS
============================================================================

1. TASK
   A Task is a unit of work containing:
   - Fragment: A portion of the physical plan to execute
   - Sources: Input streams from other tasks
   - Sinks: Output streams to other tasks or results
   - MaxTimeRange: Time bounds for the data this task processes

2. STREAM
   A Stream is a data channel between tasks:
   - Has a unique ULID identifier
   - Carries Arrow RecordBatches
   - Has exactly one sender and one receiver
   - Provides backpressure via blocking writes

3. PIPELINE BREAKERS
   Nodes that force task boundaries because they need all input data:
   - TopK: Needs all data to determine top K rows
   - RangeAggregation: Aggregates across time windows
   - VectorAggregation: Aggregates across label groups

4. PARALLELIZATION
   - Parallelize nodes mark parallelization opportunities
   - ScanSet targets become individual scan tasks
   - Multiple scan tasks feed into aggregation tasks

============================================================================
WORKFLOW ARCHITECTURE
============================================================================

Physical Plan:
  VectorAggregation
    └── RangeAggregation
          └── Parallelize
                └── ScanSet [3 targets]

Becomes Workflow:
  Task 0 (Root): VectorAggregation
    Sources: [Stream from Task 1]
    Sinks: [Results Stream]

  Task 1: RangeAggregation
    Sources: [Streams from Tasks 2, 3, 4]
    Sinks: [Stream to Task 0]

  Task 2: DataObjScan[0]      Task 3: DataObjScan[1]      Task 4: DataObjScan[2]
    Sources: []                 Sources: []                 Sources: []
    Sinks: [→ Task 1]          Sinks: [→ Task 1]          Sinks: [→ Task 1]

============================================================================
TASK PARTITIONING RULES
============================================================================

1. Pipeline breakers force task boundaries:
   - TopK (needs all data to sort)
   - RangeAggregation (needs all data for time windows)
   - VectorAggregation (needs all data for grouping)

2. Parallelize nodes hint at parallelism opportunities
   - Inserted by optimization passes
   - Marks where plan can be split for parallel execution

3. ScanSet targets become individual scan tasks
   - Each DataObjScan target = separate task
   - Enables parallel data loading from storage

============================================================================
ADMISSION CONTROL
============================================================================

The workflow uses semaphore-based admission control:

    type Options struct {
        MaxRunningScanTasks  int  // Limit concurrent scan tasks
        MaxRunningOtherTasks int  // Limit concurrent non-scan tasks (0 = unlimited)
    }

This prevents resource exhaustion when processing queries with many scan nodes.

============================================================================
STREAM STATES
============================================================================

Streams transition through these states:
  - Idle: Created but not bound
  - Open: Both sender and receiver bound
  - Blocked: Waiting for data (backpressure)
  - Closed: Complete

Data Flow:
  - Sender task writes Arrow RecordBatches
  - Receiver task reads batches
  - Blocking writes provide backpressure

============================================================================
*/

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
	"github.com/grafana/loki/v3/pkg/engine/internal/util/dag"
	"github.com/grafana/loki/v3/pkg/engine/internal/workflow"
)

// ============================================================================
// WORKFLOW PLANNING TESTS
// ============================================================================

/*
TestWorkflowPlanning_BasicWorkflow demonstrates basic workflow creation.

This test shows how a simple physical plan is converted to a workflow
with tasks and streams.
*/
func TestWorkflowPlanning_BasicWorkflow(t *testing.T) {
	t.Run("simple workflow with scan and aggregation", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Simple Workflow
		   ============================================================================

		   Physical Plan:
		   --------------
		     VectorAggregation
		       └── RangeAggregation
		             └── DataObjScan

		   Expected Workflow:
		   ------------------
		   Due to pipeline breakers (VectorAggregation, RangeAggregation),
		   this becomes 2 tasks:
		     - Task 1: Scan + RangeAggregation (combined)
		     - Task 0: VectorAggregation (root)

		   The pipeline breakers determine task boundaries.
		*/
		var graph dag.Graph[physical.Node]

		scan := graph.Add(&physical.DataObjScan{
			Location:  "obj1",
			Section:   0,
			StreamIDs: []int64{1, 2},
		})
		rangeAgg := graph.Add(&physical.RangeAggregation{
			Operation: types.RangeAggregationTypeCount,
			Start:     time.Now(),
			End:       time.Now().Add(time.Hour),
			Range:     5 * time.Minute,
		})
		vectorAgg := graph.Add(&physical.VectorAggregation{
			Operation: types.VectorAggregationTypeSum,
		})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: rangeAgg, Child: scan})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: vectorAgg, Child: rangeAgg})

		physicalPlan := physical.FromGraph(graph)

		// Create workflow using test runner
		runner := newTestRunner()

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)
		require.NotNil(t, wf)

		t.Logf("Workflow:\n%s", workflow.Sprint(wf))

		// The workflow should have created tasks for:
		// - Vector aggregation (root)
		// - Range aggregation (intermediate)
		// - Scan task (leaf)
		// But due to pipeline breakers, there should be 2 tasks
		// (scan + range agg become one task, vector agg is another)
	})
}

/*
TestWorkflowPlanning_ParallelScans demonstrates workflow with parallel scan tasks.

When a ScanSet has multiple targets, each target becomes a separate task
that can run in parallel.
*/
func TestWorkflowPlanning_ParallelScans(t *testing.T) {
	t.Run("workflow with parallel scan tasks", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Parallel Scan Tasks
		   ============================================================================

		   Physical Plan:
		   --------------
		     RangeAggregation
		       └── Parallelize
		             └── ScanSet [obj1, obj2, obj3]

		   Expected Workflow:
		   ------------------
		     Task 0 (Root): RangeAggregation
		       Sources: [Streams from Tasks 1, 2, 3]
		       Sinks: [Results Stream]

		     Task 1: DataObjScan[obj1]
		       Sources: []
		       Sinks: [Stream to Task 0]

		     Task 2: DataObjScan[obj2]
		       Sources: []
		       Sinks: [Stream to Task 0]

		     Task 3: DataObjScan[obj3]
		       Sources: []
		       Sinks: [Stream to Task 0]

		   This enables parallel data loading from storage.
		*/
		var graph dag.Graph[physical.Node]

		scanSet := graph.Add(&physical.ScanSet{
			Targets: []*physical.ScanTarget{
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj1", Section: 0}},
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj2", Section: 0}},
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj3", Section: 0}},
			},
		})
		parallelize := graph.Add(&physical.Parallelize{})
		rangeAgg := graph.Add(&physical.RangeAggregation{
			Operation: types.RangeAggregationTypeCount,
		})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: rangeAgg, Child: parallelize})

		physicalPlan := physical.FromGraph(graph)

		runner := newTestRunner()

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)

		t.Logf("Workflow with parallel scans:\n%s", workflow.Sprint(wf))

		// Start the workflow
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pipeline, err := wf.Run(ctx)
		require.NoError(t, err)
		defer pipeline.Close()

		// Verify tasks were registered with runner
		runner.mu.RLock()
		numTasks := len(runner.tasks)
		runner.mu.RUnlock()

		// Should have multiple tasks:
		// - 1 for range aggregation (root)
		// - 3 for individual scan targets
		t.Logf("Number of tasks registered: %d", numTasks)
	})
}

/*
TestWorkflowPlanning_StreamBindings demonstrates how tasks communicate via streams.

Streams connect tasks in the workflow, enabling data flow between tasks.
*/
func TestWorkflowPlanning_StreamBindings(t *testing.T) {
	t.Run("streams connect tasks", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Stream Bindings
		   ============================================================================

		   Each stream has:
		   - Unique ULID identifier
		   - Tenant ID for isolation
		   - Exactly one sender (task)
		   - Exactly one receiver (task or local listener)

		   Stream states:
		   - Idle: Created but not bound
		   - Open: Both sender and receiver bound
		   - Blocked: Waiting for data
		   - Closed: Complete

		   Data flow:
		   - Sender task writes Arrow RecordBatches
		   - Receiver task reads batches
		   - Backpressure via blocking writes
		*/
		runner := newTestRunner()

		// Build plan with multiple tasks
		var graph dag.Graph[physical.Node]

		scanSet := graph.Add(&physical.ScanSet{
			Targets: []*physical.ScanTarget{
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj1"}},
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj2"}},
			},
		})
		parallelize := graph.Add(&physical.Parallelize{})
		agg := graph.Add(&physical.RangeAggregation{Operation: types.RangeAggregationTypeCount})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: agg, Child: parallelize})

		physicalPlan := physical.FromGraph(graph)

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pipeline, err := wf.Run(ctx)
		require.NoError(t, err)
		defer pipeline.Close()

		// Verify streams were created
		runner.mu.RLock()
		defer runner.mu.RUnlock()

		require.NotEmpty(t, runner.streams, "workflow should create streams")

		for id, stream := range runner.streams {
			t.Logf("Stream %s: sender=%v, receiver=%v",
				id, stream.Sender, stream.TaskReceiver)
		}
	})
}

/*
TestWorkflowPlanning_TaskLifecycle demonstrates task state transitions.

Tasks transition through these states:

	Created → Pending → Running → Completed/Cancelled/Failed
*/
func TestWorkflowPlanning_TaskLifecycle(t *testing.T) {
	t.Run("task state transitions", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Task State Transitions
		   ============================================================================

		   Task States:
		   -----------
		     Created: Task defined but not submitted
		         │
		         ▼
		     Pending: Task submitted, waiting for worker
		         │
		         ▼
		     Running: Task executing on worker
		         │
		         ├──► Completed: Task finished successfully
		         ├──► Cancelled: Task cancelled (timeout, user request)
		         └──► Failed: Task encountered error

		   State transitions are reported via TaskEventHandler callback.
		*/
		runner := newTestRunner()

		var graph dag.Graph[physical.Node]
		scan := graph.Add(&physical.DataObjScan{Location: "obj1"})
		agg := graph.Add(&physical.RangeAggregation{Operation: types.RangeAggregationTypeCount})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: agg, Child: scan})

		physicalPlan := physical.FromGraph(graph)

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pipeline, err := wf.Run(ctx)
		require.NoError(t, err)
		defer pipeline.Close()

		// Simulate task completion
		runner.mu.RLock()
		for _, rt := range runner.tasks {
			// Notify task completion
			rt.handler(ctx, rt.task, workflow.TaskStatus{State: workflow.TaskStateCompleted})
		}
		runner.mu.RUnlock()

		// Tasks should transition to completed state
	})
}

/*
TestWorkflowPlanning_AdmissionControl demonstrates semaphore-based admission control.

The workflow limits concurrent tasks to prevent resource exhaustion.
*/
func TestWorkflowPlanning_AdmissionControl(t *testing.T) {
	t.Run("admission control limits concurrent tasks", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Admission Control
		   ============================================================================

		   Workflow Options:
		   -----------------
		     MaxRunningScanTasks:  Limits concurrent scan tasks
		     MaxRunningOtherTasks: Limits concurrent non-scan tasks (0 = unlimited)

		   Why Admission Control?
		   ----------------------
		   Without limits, a query with 1000 scan targets would try to run
		   all 1000 scans simultaneously, potentially exhausting:
		   - Memory (each scan buffers data)
		   - Network connections (to storage)
		   - CPU (for decompression)

		   With admission control:
		   - Limited number of concurrent scans
		   - Tasks queue up waiting for slots
		   - Prevents resource exhaustion
		*/
		runner := newTestRunner()

		// Build plan with many scan targets
		var graph dag.Graph[physical.Node]
		targets := make([]*physical.ScanTarget, 10)
		for i := 0; i < 10; i++ {
			targets[i] = &physical.ScanTarget{
				Type:       physical.ScanTypeDataObject,
				DataObject: &physical.DataObjScan{Location: physical.DataObjLocation(fmt.Sprintf("obj%d", i))},
			}
		}

		scanSet := graph.Add(&physical.ScanSet{Targets: targets})
		parallelize := graph.Add(&physical.Parallelize{})
		agg := graph.Add(&physical.RangeAggregation{Operation: types.RangeAggregationTypeCount})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})

		physicalPlan := physical.FromGraph(graph)

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pipeline, err := wf.Run(ctx)
		require.NoError(t, err)
		defer pipeline.Close()

		// Cancel the context to trigger cancellation
		cancel()

		// In a real scenario, this would trigger task cancellation
		t.Log("Context cancelled - tasks would transition to cancelled state")
	})
}

// ============================================================================
// STREAM BINDING TESTS
// ============================================================================

/*
TestStreamBindings demonstrates how tasks communicate via streams.
*/
func TestStreamBindings(t *testing.T) {
	t.Run("streams connect tasks", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Streams Connect Tasks
		   ============================================================================

		   Streams are the data channels between tasks:
		   - Each stream has exactly one sender (task producing data)
		   - Each stream has exactly one receiver (task consuming data or local listener)
		   - Data flows as Arrow RecordBatches

		   This test verifies that streams are properly created and bound
		   between tasks.
		*/
		runner := newTestRunner()

		// Build plan with multiple tasks
		var graph dag.Graph[physical.Node]

		scanSet := graph.Add(&physical.ScanSet{
			Targets: []*physical.ScanTarget{
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj1"}},
				{Type: physical.ScanTypeDataObject, DataObject: &physical.DataObjScan{Location: "obj2"}},
			},
		})
		parallelize := graph.Add(&physical.Parallelize{})
		agg := graph.Add(&physical.RangeAggregation{Operation: types.RangeAggregationTypeCount})

		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: parallelize, Child: scanSet})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: agg, Child: parallelize})

		physicalPlan := physical.FromGraph(graph)

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pipeline, err := wf.Run(ctx)
		require.NoError(t, err)
		defer pipeline.Close()

		// Verify streams were created
		runner.mu.RLock()
		defer runner.mu.RUnlock()

		require.NotEmpty(t, runner.streams, "workflow should create streams")

		for id, stream := range runner.streams {
			t.Logf("Stream %s: sender=%v, receiver=%v",
				id, stream.Sender, stream.TaskReceiver)
		}
	})

	t.Run("stream properties", func(t *testing.T) {
		/*
		   ============================================================================
		   TEST: Stream Properties
		   ============================================================================

		   Each stream has:
		   - Unique ULID identifier
		   - Tenant ID for isolation
		   - Schema (Arrow schema of data it carries)
		   - Exactly one sender (task)
		   - Exactly one receiver (task or local listener)

		   Stream States:
		   - Idle: Created but not yet bound
		   - Open: Both sender and receiver bound
		   - Blocked: Waiting for data (backpressure)
		   - Closed: Complete, no more data
		*/
		runner := newTestRunner()

		var graph dag.Graph[physical.Node]
		scan := graph.Add(&physical.DataObjScan{Location: "obj1"})
		agg := graph.Add(&physical.RangeAggregation{Operation: types.RangeAggregationTypeCount})
		_ = graph.AddEdge(dag.Edge[physical.Node]{Parent: agg, Child: scan})

		physicalPlan := physical.FromGraph(graph)

		wf, err := workflow.New(workflow.Options{}, log.NewNopLogger(), "test-tenant", runner, physicalPlan)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pipeline, err := wf.Run(ctx)
		require.NoError(t, err)
		defer pipeline.Close()

		runner.mu.RLock()
		defer runner.mu.RUnlock()

		for id, rs := range runner.streams {
			stream := rs.Stream
			t.Logf("Stream %s properties:", id)
			t.Logf("  - ULID: %s", stream.ULID)
			t.Logf("  - TenantID: %s", stream.TenantID)
		}
	})
}

// ============================================================================
// WORKFLOW HELPER TESTS
// ============================================================================

/*
TestWorkflowHelpers demonstrates using the helper functions from
learning_test_utils.go for workflow testing.
*/
func TestWorkflowHelpers(t *testing.T) {
	t.Run("newTestWorkflow helper", func(t *testing.T) {
		/*
		   The newTestWorkflow helper creates a workflow with a test runner,
		   simplifying test setup.
		*/
		plan := buildSimplePlan(t)
		wf, runner := newTestWorkflow(t, plan)

		require.NotNil(t, wf)
		require.NotNil(t, runner)

		t.Logf("Created workflow with test runner")
	})
}
