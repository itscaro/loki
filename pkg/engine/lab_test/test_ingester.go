package engine

/*
============================================================================
TEST INGESTER - LOCAL DATAOBJ STORAGE FOR ENGINE V2 LEARNING LAB
============================================================================

This file provides a TestIngester class that enables writing logs in the same
format as Loki's write path (tenant, labels, structured metadata, log lines)
and persists them to DataObj format using local filesystem storage.

The ingester can be used in the learning lab to:
1. Create realistic test data in DataObj format
2. Test the full Engine V2 query path with actual storage
3. Understand the DataObj write/read lifecycle

Architecture:
=============

    TestIngester
        │
        ├─► logsobj.Builder (accumulates logs)
        │       ├─► streams.Builder (tracks streams)
        │       └─► logs.Builder (stores log lines)
        │
        ├─► uploader.Uploader (writes to storage)
        │
        └─► objstore.Bucket (filesystem or in-memory)


Usage:
======

    // Create ingester with in-memory storage
    ingester, err := NewTestIngester(TestIngesterConfig{
        InMemory: true,
    })

    // Or with filesystem storage
    ingester, err := NewTestIngester(TestIngesterConfig{
        StoragePath: "/tmp/loki-lab",
    })

    // Ingest logs
    err = ingester.Push(ctx, "tenant-1", []LogEntry{
        {
            Labels:    `{app="myapp", env="prod"}`,
            Line:      "level=info msg=\"hello world\"",
            Timestamp: time.Now(),
            Metadata:  map[string]string{"traceID": "abc123"},
        },
    })

    // Flush to storage (creates DataObj file)
    paths, err := ingester.Flush(ctx)

    // Get catalog for querying
    catalog := ingester.Catalog()

============================================================================
*/

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/consumer/logsobj"
	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/dataobj/uploader"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/scratch"
)

// ============================================================================
// CONFIGURATION
// ============================================================================

// TestIngesterConfig configures a TestIngester.
type TestIngesterConfig struct {
	// InMemory uses in-memory storage instead of filesystem.
	// When true, StoragePath is ignored.
	InMemory bool

	// StoragePath is the directory to store DataObj files.
	// Required when InMemory is false.
	StoragePath string

	// TargetObjectSize is the target size for DataObj files.
	// Default: 10MB (small for testing)
	TargetObjectSize int

	// TargetSectionSize is the target size for sections within DataObj.
	// Default: 1MB (small for testing)
	TargetSectionSize int

	// BufferSize is the size of the in-memory buffer before sorting.
	// Default: 1MB (small for testing)
	BufferSize int
}

// DefaultTestIngesterConfig returns a config suitable for testing.
func DefaultTestIngesterConfig() TestIngesterConfig {
	return TestIngesterConfig{
		InMemory:          true,
		TargetObjectSize:  10 * 1024 * 1024, // 10MB
		TargetSectionSize: 1 * 1024 * 1024,  // 1MB
		BufferSize:        1 * 1024 * 1024,  // 1MB
	}
}

// Validate validates the configuration.
func (cfg *TestIngesterConfig) Validate() error {
	if !cfg.InMemory && cfg.StoragePath == "" {
		return fmt.Errorf("StoragePath is required when InMemory is false")
	}
	if cfg.TargetObjectSize <= 0 {
		cfg.TargetObjectSize = 10 * 1024 * 1024
	}
	if cfg.TargetSectionSize <= 0 {
		cfg.TargetSectionSize = 1 * 1024 * 1024
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1 * 1024 * 1024
	}
	return nil
}

// ============================================================================
// LOG ENTRY
// ============================================================================

// LogEntry represents a single log entry to ingest.
type LogEntry struct {
	// Labels is the stream labels in LogQL format (e.g., `{app="test", env="prod"}`).
	Labels string

	// Line is the log line content.
	Line string

	// Timestamp is the log entry timestamp.
	Timestamp time.Time

	// Metadata is optional structured metadata (key-value pairs).
	Metadata map[string]string
}

// ============================================================================
// TEST INGESTER
// ============================================================================

// TestIngester provides a simple interface for ingesting logs into DataObj format.
type TestIngester struct {
	mu sync.Mutex

	cfg      TestIngesterConfig
	logger   log.Logger
	bucket   objstore.Bucket
	uploader *uploader.Uploader
	builder  *logsobj.Builder

	// Track uploaded object paths for cleanup
	uploadedPaths []string

	// Catalog for querying
	metastore *metastore.ObjectMetastore
	catalog   physical.Catalog
}

// NewTestIngester creates a new TestIngester.
func NewTestIngester(cfg TestIngesterConfig) (*TestIngester, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "component", "test-ingester")

	// Create bucket
	var bucket objstore.Bucket
	if cfg.InMemory {
		bucket = objstore.NewInMemBucket()
	} else {
		// Ensure directory exists
		if err := os.MkdirAll(cfg.StoragePath, 0755); err != nil {
			return nil, fmt.Errorf("creating storage directory: %w", err)
		}
		var err error
		bucket, err = filesystem.NewBucket(cfg.StoragePath)
		if err != nil {
			return nil, fmt.Errorf("creating filesystem bucket: %w", err)
		}
	}

	// Create builder config
	builderCfg := logsobj.BuilderConfig{
		BuilderBaseConfig: logsobj.BuilderBaseConfig{
			TargetPageSize:          flagext.Bytes(256 * 1024), // 256KB pages
			TargetObjectSize:        flagext.Bytes(cfg.TargetObjectSize),
			TargetSectionSize:       flagext.Bytes(cfg.TargetSectionSize),
			BufferSize:              flagext.Bytes(cfg.BufferSize),
			SectionStripeMergeLimit: 2,
		},
	}

	// Create builder
	builder, err := logsobj.NewBuilder(builderCfg, scratch.NewMemory())
	if err != nil {
		return nil, fmt.Errorf("creating builder: %w", err)
	}

	// Create uploader
	uploaderCfg := uploader.Config{
		SHAPrefixSize: 2,
	}
	up := uploader.New(uploaderCfg, bucket, logger)

	// Create metastore for querying
	ms := metastore.NewObjectMetastore(bucket, logger, nil)

	return &TestIngester{
		cfg:       cfg,
		logger:    logger,
		bucket:    bucket,
		uploader:  up,
		builder:   builder,
		metastore: ms,
		catalog:   NewTestCatalog(ms),
	}, nil
}

// Push ingests log entries for a tenant.
func (ti *TestIngester) Push(ctx context.Context, tenant string, entries []LogEntry) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	// Group entries by labels (stream)
	streamsByLabels := make(map[string][]LogEntry)
	for _, entry := range entries {
		streamsByLabels[entry.Labels] = append(streamsByLabels[entry.Labels], entry)
	}

	// Append each stream
	for labelsStr, streamEntries := range streamsByLabels {
		// Convert to logproto.Stream
		protoEntries := make([]logproto.Entry, len(streamEntries))
		for i, entry := range streamEntries {
			// Convert metadata to push.LabelsAdapter
			var metadata push.LabelsAdapter
			if len(entry.Metadata) > 0 {
				metadata = make(push.LabelsAdapter, 0, len(entry.Metadata))
				for k, v := range entry.Metadata {
					metadata = append(metadata, push.LabelAdapter{Name: k, Value: v})
				}
			}

			protoEntries[i] = logproto.Entry{
				Timestamp:          entry.Timestamp,
				Line:               entry.Line,
				StructuredMetadata: metadata,
			}
		}

		stream := logproto.Stream{
			Labels:  labelsStr,
			Entries: protoEntries,
		}

		if err := ti.builder.Append(tenant, stream); err != nil {
			if err == logsobj.ErrBuilderFull {
				// Flush and retry
				if _, flushErr := ti.flush(ctx); flushErr != nil {
					return fmt.Errorf("auto-flush failed: %w", flushErr)
				}
				// Retry append
				if err := ti.builder.Append(tenant, stream); err != nil {
					return fmt.Errorf("appending stream after flush: %w", err)
				}
			} else {
				return fmt.Errorf("appending stream: %w", err)
			}
		}
	}

	return nil
}

// PushStream is a convenience method to push a single stream.
func (ti *TestIngester) PushStream(ctx context.Context, tenant, labels string, lines []string, timestamps []time.Time) error {
	if len(lines) != len(timestamps) {
		return fmt.Errorf("lines and timestamps must have same length")
	}

	entries := make([]LogEntry, len(lines))
	for i := range lines {
		entries[i] = LogEntry{
			Labels:    labels,
			Line:      lines[i],
			Timestamp: timestamps[i],
		}
	}

	return ti.Push(ctx, tenant, entries)
}

// PushSimple is a convenience method to push lines with auto-generated timestamps.
func (ti *TestIngester) PushSimple(ctx context.Context, tenant, labels string, lines []string) error {
	now := time.Now()
	timestamps := make([]time.Time, len(lines))
	for i := range lines {
		timestamps[i] = now.Add(time.Duration(i) * time.Millisecond)
	}
	return ti.PushStream(ctx, tenant, labels, lines, timestamps)
}

// Flush flushes buffered data to storage and returns the paths of created objects.
func (ti *TestIngester) Flush(ctx context.Context) ([]string, error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	return ti.flush(ctx)
}

func (ti *TestIngester) flush(ctx context.Context) ([]string, error) {
	obj, closer, err := ti.builder.Flush()
	if err != nil {
		if err == logsobj.ErrBuilderEmpty {
			return nil, nil // Nothing to flush
		}
		return nil, fmt.Errorf("flushing builder: %w", err)
	}
	defer closer.Close()

	// Upload to bucket
	path, err := ti.uploader.Upload(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("uploading object: %w", err)
	}

	ti.uploadedPaths = append(ti.uploadedPaths, path)
	return []string{path}, nil
}

// Bucket returns the underlying object storage bucket.
func (ti *TestIngester) Bucket() objstore.Bucket {
	return ti.bucket
}

// Catalog returns a physical.Catalog for querying ingested data.
func (ti *TestIngester) Catalog() physical.Catalog {
	return ti.catalog
}

// Metastore returns the underlying ObjectMetastore.
func (ti *TestIngester) Metastore() *metastore.ObjectMetastore {
	return ti.metastore
}

// UploadedPaths returns the paths of all uploaded DataObj files.
func (ti *TestIngester) UploadedPaths() []string {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	return append([]string{}, ti.uploadedPaths...)
}

// Close cleans up resources.
func (ti *TestIngester) Close() error {
	// Nothing to clean up for in-memory bucket
	if ti.cfg.InMemory {
		return nil
	}

	// For filesystem bucket, optionally clean up
	if bucket, ok := ti.bucket.(io.Closer); ok {
		return bucket.Close()
	}
	return nil
}

// Cleanup removes all uploaded objects from storage.
func (ti *TestIngester) Cleanup(ctx context.Context) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	for _, path := range ti.uploadedPaths {
		if err := ti.bucket.Delete(ctx, path); err != nil {
			return fmt.Errorf("deleting %s: %w", path, err)
		}
	}
	ti.uploadedPaths = nil
	return nil
}

// ============================================================================
// TEST CATALOG - WRAPS OBJECT METASTORE FOR ENGINE V2
// ============================================================================

// TestCatalog wraps ObjectMetastore to implement physical.Catalog for testing.
type TestCatalog struct {
	metastore *metastore.ObjectMetastore
}

// NewTestCatalog creates a new TestCatalog wrapping the given metastore.
func NewTestCatalog(ms *metastore.ObjectMetastore) *TestCatalog {
	return &TestCatalog{metastore: ms}
}

// ResolveShardDescriptors implements physical.Catalog.
func (c *TestCatalog) ResolveShardDescriptors(
	selector physical.Expression,
	from, through time.Time,
) ([]physical.FilteredShardDescriptor, error) {
	return c.ResolveShardDescriptorsWithShard(selector, nil, physical.ShardInfo{Shard: 0, Of: 1}, from, through)
}

// ResolveShardDescriptorsWithShard implements physical.Catalog.
func (c *TestCatalog) ResolveShardDescriptorsWithShard(
	selector physical.Expression,
	predicates []physical.Expression,
	shard physical.ShardInfo,
	from, through time.Time,
) ([]physical.FilteredShardDescriptor, error) {
	ctx := context.Background()

	// Convert physical.Expression to label matchers
	matchers, err := expressionToMatchers(selector)
	if err != nil {
		return nil, fmt.Errorf("converting selector to matchers: %w", err)
	}

	// Convert predicates to matchers
	var predicateMatchers []*labels.Matcher
	for _, pred := range predicates {
		pm, err := expressionToMatchers(pred)
		if err != nil {
			// Skip predicates that can't be converted
			continue
		}
		predicateMatchers = append(predicateMatchers, pm...)
	}

	// Query metastore
	sections, err := c.metastore.Sections(ctx, from, through, matchers, predicateMatchers)
	if err != nil {
		return nil, fmt.Errorf("querying metastore: %w", err)
	}

	// Convert to FilteredShardDescriptor
	var result []physical.FilteredShardDescriptor
	for i, sec := range sections {
		// Apply sharding
		if shard.Of > 1 && i%int(shard.Of) != int(shard.Shard) {
			continue
		}

		result = append(result, physical.FilteredShardDescriptor{
			Location:  physical.DataObjLocation(sec.SectionKey.ObjectPath),
			Streams:   sec.StreamIDs,
			Sections:  []int{int(sec.SectionKey.SectionIdx)},
			TimeRange: physical.TimeRange{Start: sec.Start, End: sec.End},
		})
	}

	return result, nil
}

// expressionToMatchers converts a physical.Expression to label matchers.
// This is a simplified implementation that handles common patterns.
func expressionToMatchers(expr physical.Expression) ([]*labels.Matcher, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *physical.BinaryExpr:
		// Check if this is an AND expression (combining two predicates)
		if e.Op == types.BinaryOpAnd {
			// Recursively handle AND expressions
			leftMatchers, err := expressionToMatchers(e.Left)
			if err != nil {
				return nil, err
			}
			rightMatchers, err := expressionToMatchers(e.Right)
			if err != nil {
				return nil, err
			}
			return append(leftMatchers, rightMatchers...), nil
		}

		// Handle simple binary expressions like label = "value"
		colExpr, ok := e.Left.(*physical.ColumnExpr)
		if !ok {
			return nil, fmt.Errorf("expected column expression on left side")
		}

		litExpr, ok := e.Right.(*physical.LiteralExpr)
		if !ok {
			return nil, fmt.Errorf("expected literal expression on right side")
		}

		value, ok := litExpr.Value().(string)
		if !ok {
			return nil, fmt.Errorf("expected string literal value, got %T", litExpr.Value())
		}

		var matchType labels.MatchType
		switch e.Op {
		case types.BinaryOpEq:
			matchType = labels.MatchEqual
		case types.BinaryOpNeq:
			matchType = labels.MatchNotEqual
		case types.BinaryOpMatchRe:
			matchType = labels.MatchRegexp
		case types.BinaryOpNotMatchRe:
			matchType = labels.MatchNotRegexp
		default:
			return nil, fmt.Errorf("unsupported binary operator: %v", e.Op)
		}

		matcher, err := labels.NewMatcher(matchType, colExpr.Ref.Column, value)
		if err != nil {
			return nil, fmt.Errorf("creating matcher: %w", err)
		}
		return []*labels.Matcher{matcher}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

var _ physical.Catalog = (*TestCatalog)(nil)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// CreateTestIngesterWithData creates a TestIngester and populates it with test data.
// Returns the ingester with data already flushed to storage.
func CreateTestIngesterWithData(ctx context.Context, tenant string, data map[string][]string) (*TestIngester, error) {
	ingester, err := NewTestIngester(DefaultTestIngesterConfig())
	if err != nil {
		return nil, err
	}

	for labels, lines := range data {
		if err := ingester.PushSimple(ctx, tenant, labels, lines); err != nil {
			ingester.Close()
			return nil, fmt.Errorf("pushing data: %w", err)
		}
	}

	if _, err := ingester.Flush(ctx); err != nil {
		ingester.Close()
		return nil, fmt.Errorf("flushing: %w", err)
	}

	return ingester, nil
}

// TempDirIngester creates a TestIngester with filesystem storage in a temp directory.
// The caller is responsible for cleaning up the directory.
func TempDirIngester() (*TestIngester, string, error) {
	dir, err := os.MkdirTemp("", "loki-lab-*")
	if err != nil {
		return nil, "", err
	}

	ingester, err := NewTestIngester(TestIngesterConfig{
		StoragePath: dir,
	})
	if err != nil {
		os.RemoveAll(dir)
		return nil, "", err
	}

	return ingester, dir, nil
}

// InMemoryIngester creates a TestIngester with in-memory storage.
func InMemoryIngester() (*TestIngester, error) {
	return NewTestIngester(DefaultTestIngesterConfig())
}

// ============================================================================
// DATAOBJ INSPECTION HELPERS
// ============================================================================

// ListDataObjects returns all DataObj files in the bucket.
func (ti *TestIngester) ListDataObjects(ctx context.Context) ([]string, error) {
	var objects []string
	err := ti.bucket.Iter(ctx, "objects/", func(name string) error {
		// Check if it's a DataObj file (under objects/ prefix)
		if filepath.Dir(name) != "objects" {
			objects = append(objects, name)
		}
		return nil
	}, objstore.WithRecursiveIter())
	return objects, err
}

// OpenDataObject opens a DataObj file from storage for inspection.
func (ti *TestIngester) OpenDataObject(ctx context.Context, path string) (*dataobj.Object, error) {
	return dataobj.FromBucket(ctx, ti.bucket, path)
}
