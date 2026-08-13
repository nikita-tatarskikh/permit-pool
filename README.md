# permit-pool

`permit-pool` is a small, allocation-free admission limiter for workloads partitioned by a string key, such as PostgreSQL schemas.

Each key is deterministically assigned to a bucket. Every bucket has an independent concurrency limit, so a blocked schema can consume only its bucket's permits instead of exhausting the entire database connection pool.

```text
schema name → FNV-1a hash → bucket → permit → database query
```

## Usage

Create one pool and acquire a permit before calling `pgxpool`:

```go
pool, err := permitpool.New(permitpool.Config{
	BucketCount:      64,
	PermitsPerBucket: 4,
})
if err != nil {
	return err
}

permit, err := pool.Acquire(ctx, schema)
if err != nil {
	return err
}
defer permit.Release()

return queryDatabase(ctx, schema)
```

The context controls how long acquisition may wait. A cancelled acquisition does not consume capacity.

Acquire the permit before requesting a database connection. Release it only after the operation can no longer retain that connection. For streaming rows, this means holding the permit until the rows are exhausted or closed.

## Configuration

`BucketCount` and `PermitsPerBucket` must both be positive. The maximum admitted concurrency is:

```text
BucketCount × PermitsPerBucket
```

For a dedicated `pgxpool` with 256 connections, a reasonable load-test candidate is:

```go
permitpool.Config{
	BucketCount:      64,
	PermitsPerBucket: 4,
}
```

This permits all 256 connections to be used while limiting one saturated bucket to four concurrent operations. It is not a universal default: fewer buckets allow larger per-bucket bursts but increase the impact of one blocked schema; more buckets improve isolation but increase contention caused by hash collisions.

Choose the values using production-shaped load tests. Pay particular attention to permit wait p95/p99, connection-pool utilization, hot schemas, deliberate bucket collisions, and blocked-query scenarios.

## Permit ownership

`Permit` is an allocation-free, single-owner value. Keep it in one local variable and call `defer permit.Release()` immediately after acquisition.

Do not copy a permit, pass it by value, store value copies, or call `Release` concurrently. Repeated sequential calls to `Release` on the original value are safe. This ownership discipline is the trade-off for a zero-allocation hot path.

## Properties

- Deterministic FNV-1a schema hashing.
- Independent concurrency limits per bucket.
- Context-aware waiting and cancellation.
- Allocation-free uncontended acquire/release path.
- No database, metrics, or framework dependencies.
- Immutable configuration after construction.

## Development

```sh
go test ./...
go test -race ./...
go test -run '^$' -bench . -benchmem -count 5
golangci-lint run
```

On an Intel Core i7-9750H with Go 1.21, uncontended `Acquire` plus `Release` measured approximately 25–28 ns/op with 0 B/op and 0 allocations/op. Benchmark results depend on the machine and Go version.
