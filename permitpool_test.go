package permitpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestPool(t *testing.T, buckets, permits int) *Pool {
	t.Helper()
	pool, err := New(Config{BucketCount: buckets, PermitsPerBucket: permits})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return pool
}

func TestNewValidatesConfig(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   error
	}{
		{"zero buckets", Config{BucketCount: 0, PermitsPerBucket: 1}, ErrInvalidBucketCount},
		{"negative buckets", Config{BucketCount: -1, PermitsPerBucket: 1}, ErrInvalidBucketCount},
		{"zero permits", Config{BucketCount: 1, PermitsPerBucket: 0}, ErrInvalidPermitsPerBucket},
		{"negative permits", Config{BucketCount: 1, PermitsPerBucket: -1}, ErrInvalidPermitsPerBucket},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.config)
			if !errors.Is(err, test.want) {
				t.Fatalf("New() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestBucketIsStableInRangeAndDistributed(t *testing.T) {
	pool := newTestPool(t, 17, 1)
	seen := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		schema := fmt.Sprintf("tenant_%d", i)
		got := pool.Bucket(schema)
		if got < 0 || got >= 17 {
			t.Fatalf("Bucket(%q) = %d, outside [0,17)", schema, got)
		}
		if again := pool.Bucket(schema); again != got {
			t.Fatalf("Bucket(%q) changed from %d to %d", schema, got, again)
		}
		seen[got] = true
	}
	if len(seen) != 17 {
		t.Fatalf("used %d buckets, want all 17", len(seen))
	}
}

func TestSimilarSchemaBucketExamples(t *testing.T) {
	const bucketCount = 8

	pool := newTestPool(t, bucketCount, 32)
	schemas := []string{
		"",
		"tenant_0001",
		"tenant_0002",
		"tenant_0003",
		"tenant_0004",
		"tenant_0010",
		"tenant_0100",
		"tenant_1000",
		"tenant_0001a",
		"tenant_0001b",
		"tenant-0001",
		"Tenant_0001",
		"tenant_0001_test",
	}

	t.Logf("%-24s  %-20s  %s", "schema", "hash", "bucket")

	for _, schema := range schemas {
		hash := hashSchema(schema)
		bucket := pool.Bucket(schema)

		if bucket < 0 || bucket >= bucketCount {
			t.Fatalf("Bucket(%q) = %d, outside [0,%d)", schema, bucket, bucketCount)
		}

		if repeated := pool.Bucket(schema); repeated != bucket {
			t.Fatalf("Bucket(%q) changed from %d to %d", schema, bucket, repeated)
		}

		t.Logf("%-24s  0x%016x  %d", schema, hash, bucket)
	}
}

func TestBucketCapacityCollisionAndIndependence(t *testing.T) {
	pool := newTestPool(t, 8, 1)
	first, collision, independent := schemasForBuckets(t, pool)

	release, err := pool.Acquire(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	defer release.Release()

	blockedCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(blockedCtx, collision); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("colliding Acquire() error = %v", err)
	}

	otherRelease, err := pool.Acquire(context.Background(), independent)
	if err != nil {
		t.Fatalf("independent Acquire() error = %v", err)
	}
	otherRelease.Release()
}

func TestWaitingAcquireProceedsAfterRelease(t *testing.T) {
	pool := newTestPool(t, 1, 1)
	release, _ := pool.Acquire(context.Background(), "one")
	acquired := make(chan Permit, 1)
	go func() {
		next, err := pool.Acquire(context.Background(), "two")
		if err == nil {
			acquired <- next
		}
	}()

	select {
	case <-acquired:
		t.Fatal("second acquisition passed a saturated bucket")
	case <-time.After(10 * time.Millisecond):
	}
	release.Release()
	select {
	case next := <-acquired:
		next.Release()
	case <-time.After(time.Second):
		t.Fatal("second acquisition did not proceed after release")
	}
}

func TestCanceledContextsDoNotConsumeCapacity(t *testing.T) {
	pool := newTestPool(t, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if permit, err := pool.Acquire(ctx, "schema"); !errors.Is(err, context.Canceled) || permit.bucket != nil {
		t.Fatalf("Acquire() = (%v, %v), want (zero, context.Canceled)", permit, err)
	}
	release, err := pool.Acquire(context.Background(), "schema")
	if err != nil {
		t.Fatalf("capacity consumed by canceled acquire: %v", err)
	}
	release.Release()
}

func TestReleaseIsIdempotent(t *testing.T) {
	pool := newTestPool(t, 1, 1)
	release, _ := pool.Acquire(context.Background(), "schema")
	for i := 0; i < 20; i++ {
		release.Release()
	}

	one, _ := pool.Acquire(context.Background(), "schema")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx, "schema"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capacity inflated after repeated release: %v", err)
	}
	one.Release()
}

func TestAcquireCancelReleaseRace(t *testing.T) {
	pool := newTestPool(t, 1, 1)
	for i := 0; i < 500; i++ {
		held, _ := pool.Acquire(context.Background(), "schema")
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan Permit, 1)
		done := make(chan struct{})
		go func() {
			release, _ := pool.Acquire(ctx, "schema")
			result <- release
			close(done)
		}()
		go cancel()
		held.Release()
		<-done
		if permit := <-result; permit.bucket != nil {
			permit.Release()
		}
	}
	release, err := pool.Acquire(context.Background(), "schema")
	if err != nil {
		t.Fatalf("Acquire() after races = %v", err)
	}
	release.Release()
}

func TestPoolNeverExceedsBucketCapacity(t *testing.T) {
	pool := newTestPool(t, 1, 3)
	var current atomic.Int64
	var maximum atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			release, err := pool.Acquire(context.Background(), "schema")
			if err != nil {
				return
			}

			now := current.Add(1)
			setMaximum(&maximum, now)

			time.Sleep(time.Microsecond)
			current.Add(-1)
			release.Release()
		}()
	}
	wg.Wait()
	if got := maximum.Load(); got > 3 {
		t.Fatalf("maximum concurrent holders = %d, want <= 3", got)
	}
}

func setMaximum(maximum *atomic.Int64, candidate int64) {
	for current := maximum.Load(); candidate > current; current = maximum.Load() {
		if maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func schemasForBuckets(t *testing.T, pool *Pool) (first, collision, independent string) {
	t.Helper()
	first = "schema_0"
	target := pool.Bucket(first)
	for i := 1; collision == "" || independent == ""; i++ {
		candidate := fmt.Sprintf("schema_%d", i)
		if pool.Bucket(candidate) == target && collision == "" {
			collision = candidate
		}
		if pool.Bucket(candidate) != target && independent == "" {
			independent = candidate
		}
	}
	return first, collision, independent
}
