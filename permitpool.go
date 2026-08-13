// Package permitpool provides bounded admission control for work partitioned
// by schema name. Acquire a permit before acquiring a database connection.
package permitpool

import (
	"context"
	"errors"
	"sync/atomic"
)

const (
	fnvOffset64      = uint64(14695981039346656037)
	fnvPrime64       = uint64(1099511628211)
	cacheLinePadding = 48
)

var (
	// ErrInvalidBucketCount means Config.BucketCount is not positive.
	ErrInvalidBucketCount = errors.New("permitpool: bucket count must be positive")

	// ErrInvalidPermitsPerBucket means Config.PermitsPerBucket is not positive.
	ErrInvalidPermitsPerBucket = errors.New("permitpool: permits per bucket must be positive")
)

// Config is copied by New. A Pool cannot be resized after construction.
//
// The application should ensure BucketCount*PermitsPerBucket leaves suitable
// headroom below pgxpool.MaxConns. Select values with workload load tests.
type Config struct {
	BucketCount      int
	PermitsPerBucket int
}

type bucket struct {
	available atomic.Int64
	wake      chan struct{}

	// Keep atomic counters of adjacent buckets on separate 64-byte cache lines.
	_ [cacheLinePadding]byte
}

// Pool is a fixed collection of independently limited hash buckets.
type Pool struct {
	buckets    []bucket
	bucketMask uint64
}

// Permit represents one successful acquisition and has single-owner semantics.
//
// Keep a Permit in one local variable and call Release from the same goroutine.
// Do not copy it, pass it by value, store copies of it, or call its methods
// concurrently. Release is idempotent only for the original Permit value;
// releasing a copy would incorrectly add capacity to the bucket again.
//
// The intended usage is an immediate defer after a successful Acquire:
//
//	permit, err := pool.Acquire(ctx, schema)
//	if err != nil {
//		return err
//	}
//	defer permit.Release()
type Permit struct {
	bucket   *bucket
	released bool
}

// Release returns capacity to the permit's bucket exactly once, provided the
// Permit has not been copied. See Permit for its single-owner contract.
func (p *Permit) Release() {
	if p.bucket == nil || p.released {
		return
	}

	p.released = true
	p.bucket.available.Add(1)
	p.bucket.notifyWaiter()
}

// New constructs an immutable permit pool.
func New(config Config) (*Pool, error) {
	if config.BucketCount <= 0 {
		return nil, ErrInvalidBucketCount
	}
	if config.PermitsPerBucket <= 0 {
		return nil, ErrInvalidPermitsPerBucket
	}

	pool := &Pool{
		buckets: make([]bucket, config.BucketCount),
	}

	if config.BucketCount&(config.BucketCount-1) == 0 {
		pool.bucketMask = uint64(config.BucketCount - 1)
	}

	for i := range pool.buckets {
		bucket := &pool.buckets[i]
		bucket.available.Store(int64(config.PermitsPerBucket))
		bucket.wake = make(chan struct{}, 1)
	}

	return pool, nil
}

// Bucket returns the stable bucket index for schema.
func (p *Pool) Bucket(schema string) int {
	hash := hashSchema(schema)

	if p.bucketMask != 0 || len(p.buckets) == 1 {
		return int(hash & p.bucketMask)
	}

	return int(hash % uint64(len(p.buckets)))
}

// Acquire waits for capacity in schema's bucket. It does not acquire a
// database connection. A successful call returns a live, single-owner Permit.
func (p *Pool) Acquire(ctx context.Context, schema string) (Permit, error) {
	if err := ctx.Err(); err != nil {
		return Permit{}, err
	}
	bucket := &p.buckets[p.Bucket(schema)]
	if bucket.tryAcquire() {
		return Permit{bucket: bucket}, nil
	}

	return acquireSlow(ctx, bucket)
}

// acquireSlow is kept out of Acquire so the uncontended path stays small and
// inlineable. It is reached only when a bucket is saturated.
func acquireSlow(ctx context.Context, bucket *bucket) (Permit, error) {
	for {
		select {
		case <-bucket.wake:
			if !bucket.tryAcquire() {
				continue
			}

			if bucket.available.Load() > 0 {
				bucket.notifyWaiter()
			}

			return Permit{bucket: bucket}, nil

		case <-ctx.Done():
			return Permit{}, ctx.Err()
		}
	}
}

func (b *bucket) tryAcquire() bool {
	for available := b.available.Load(); available > 0; {
		if b.available.CompareAndSwap(available, available-1) {
			return true
		}

		available = b.available.Load()
	}

	return false
}

func (b *bucket) notifyWaiter() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func hashSchema(schema string) uint64 {
	hash := fnvOffset64

	for i := 0; i < len(schema); i++ {
		hash ^= uint64(schema[i])
		hash *= fnvPrime64
	}

	return hash
}
