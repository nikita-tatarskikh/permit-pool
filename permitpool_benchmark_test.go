package permitpool

import (
	"context"
	"sync/atomic"
	"testing"
)

func BenchmarkBucket(b *testing.B) {
	pool, _ := New(Config{BucketCount: 64, PermitsPerBucket: 3})
	const schema = "tenant_0123456789abcdef"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = pool.Bucket(schema)
	}
}

func BenchmarkAcquireRelease(b *testing.B) {
	pool, _ := New(Config{BucketCount: 64, PermitsPerBucket: 3})
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		release, err := pool.Acquire(ctx, "tenant_42")
		if err != nil {
			b.Fatal(err)
		}
		release.Release()
	}
}

func BenchmarkAcquireReleaseParallelHotBucket(b *testing.B) {
	pool, _ := New(Config{BucketCount: 64, PermitsPerBucket: 256})
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			release, err := pool.Acquire(ctx, "tenant_42")
			if err != nil {
				b.Error(err)
				return
			}
			release.Release()
		}
	})
}

func BenchmarkAcquireReleaseParallelContended(b *testing.B) {
	pool, _ := New(Config{BucketCount: 64, PermitsPerBucket: 3})
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			permit, err := pool.Acquire(ctx, "tenant_42")
			if err != nil {
				b.Error(err)
				return
			}
			permit.Release()
		}
	})
}

func BenchmarkAcquireReleaseParallelDistributed(b *testing.B) {
	pool, _ := New(Config{BucketCount: 64, PermitsPerBucket: 8})
	ctx := context.Background()
	schemas := make([]string, 64)
	for i := range schemas {
		schemas[i] = string(rune(i + 1))
	}
	var workerID atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		schema := schemas[workerID.Add(1)%uint64(len(schemas))]
		for pb.Next() {
			release, err := pool.Acquire(ctx, schema)
			if err != nil {
				b.Error(err)
				return
			}
			release.Release()
		}
	})
}
