//go:build integration

package permitpool

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const integrationTimeout = 2 * time.Second

func TestIntegrationBlockedQueriesExhaustUnprotectedPGXPool(t *testing.T) {
	databaseURL := startPostgresContainer(t)
	admin := connectIntegrationAdmin(t, databaseURL)
	createIntegrationSchemas(t, admin)
	lock := lockIntegrationTable(t, databaseURL, "blocked_schema")
	defer lock.Close(context.Background())

	databasePool := newIntegrationPGXPool(t, databaseURL, 2)
	defer databasePool.Close()

	blockedContext, cancelBlocked := context.WithCancel(context.Background())
	defer cancelBlocked()

	blocked := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var value int
			blocked <- databasePool.QueryRow(
				blockedContext,
				`select value from blocked_schema.items where id = 1`,
			).Scan(&value)
		}()
	}
	waitForAcquiredConnections(t, databasePool, 2)

	healthyContext, cancelHealthy := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelHealthy()
	var value int
	err := databasePool.QueryRow(
		healthyContext,
		`select value from healthy_schema.items where id = 1`,
	).Scan(&value)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("healthy query error = %v, want context deadline while pgxpool is exhausted", err)
	}

	cancelBlocked()
	for i := 0; i < 2; i++ {
		<-blocked
	}
}

func TestIntegrationPermitPoolContainsBlockedSchema(t *testing.T) {
	databaseURL := startPostgresContainer(t)
	admin := connectIntegrationAdmin(t, databaseURL)
	createIntegrationSchemas(t, admin)

	databasePool := newIntegrationPGXPool(t, databaseURL, 2)
	defer databasePool.Close()

	permits := newTestPool(t, 2, 1)
	blockedSchema, healthySchema := schemasInDifferentBuckets(t, permits)
	createSchemaAlias(t, admin, blockedSchema, "blocked_schema")
	createSchemaAlias(t, admin, healthySchema, "healthy_schema")
	lock := lockIntegrationTable(t, databaseURL, "blocked_schema")
	defer lock.Close(context.Background())

	blockedContext, cancelBlocked := context.WithCancel(context.Background())
	defer cancelBlocked()
	blocked := make(chan error, 1)
	go func() {
		blocked <- queryIntegrationValue(blockedContext, permits, databasePool, blockedSchema)
	}()
	waitForAcquiredConnections(t, databasePool, 1)

	sameBucketContext, cancelSameBucket := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelSameBucket()
	err := queryIntegrationValue(sameBucketContext, permits, databasePool, blockedSchema)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second blocked-schema query error = %v, want permit wait deadline", err)
	}
	if acquired := databasePool.Stat().AcquiredConns(); acquired != 1 {
		t.Fatalf("acquired database connections = %d, want only the admitted blocked query", acquired)
	}

	healthyContext, cancelHealthy := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancelHealthy()
	if err := queryIntegrationValue(healthyContext, permits, databasePool, healthySchema); err != nil {
		t.Fatalf("query in independent bucket failed: %v", err)
	}

	cancelBlocked()
	<-blocked
}

func queryIntegrationValue(
	ctx context.Context,
	permits *Pool,
	databasePool *pgxpool.Pool,
	schema string,
) error {
	permit, err := permits.Acquire(ctx, schema)
	if err != nil {
		return err
	}
	defer permit.Release()

	var value int
	return databasePool.QueryRow(
		ctx,
		fmt.Sprintf(`select value from %s.items where id = 1`, pgx.Identifier{schema}.Sanitize()),
	).Scan(&value)
}

func startPostgresContainer(t *testing.T) string {
	t.Helper()

	containerID := dockerCommand(t,
		"run", "--detach", "--rm",
		"--env", "POSTGRES_PASSWORD=postgres",
		"--env", "POSTGRES_DB=permit_pool",
		"--publish", "127.0.0.1::5432",
		"postgres:16-alpine",
	)
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "--force", containerID).Run()
	})

	address := dockerCommand(t, "port", containerID, "5432/tcp")
	databaseURL := fmt.Sprintf("postgres://postgres:postgres@%s/permit_pool?sslmode=disable", address)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		connection, err := pgx.Connect(ctx, databaseURL)
		cancel()
		if err == nil {
			connection.Close(context.Background())
			return databaseURL
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("PostgreSQL container did not become ready within 30 seconds")
	return ""
}

func dockerCommand(t *testing.T, arguments ...string) string {
	t.Helper()
	output, err := exec.Command("docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func connectIntegrationAdmin(t *testing.T, databaseURL string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close(context.Background()) })
	return connection
}

func newIntegrationPGXPool(t *testing.T, databaseURL string, maximumConnections int32) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maximumConnections
	config.MinConns = 0
	databasePool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	return databasePool
}

func createIntegrationSchemas(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	_, err := admin.Exec(context.Background(), `
		create schema blocked_schema;
		create schema healthy_schema;
		create table blocked_schema.items (id integer primary key, value integer not null);
		create table healthy_schema.items (id integer primary key, value integer not null);
		insert into blocked_schema.items values (1, 10);
		insert into healthy_schema.items values (1, 20);
	`)
	if err != nil {
		t.Fatal(err)
	}
}

func createSchemaAlias(t *testing.T, admin *pgx.Conn, alias, source string) {
	t.Helper()
	_, err := admin.Exec(context.Background(), fmt.Sprintf(
		`create schema %s; create view %s.items as select * from %s.items`,
		pgx.Identifier{alias}.Sanitize(),
		pgx.Identifier{alias}.Sanitize(),
		pgx.Identifier{source}.Sanitize(),
	))
	if err != nil {
		t.Fatal(err)
	}
}

func lockIntegrationTable(t *testing.T, databaseURL, schema string) *pgx.Conn {
	t.Helper()
	connection := connectIntegrationAdmin(t, databaseURL)
	_, err := connection.Exec(context.Background(), fmt.Sprintf(
		`begin; lock table %s.items in access exclusive mode`,
		pgx.Identifier{schema}.Sanitize(),
	))
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func waitForAcquiredConnections(t *testing.T, databasePool *pgxpool.Pool, expected int32) {
	t.Helper()
	deadline := time.Now().Add(integrationTimeout)
	for time.Now().Before(deadline) {
		if databasePool.Stat().AcquiredConns() == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("acquired connections = %d, want %d", databasePool.Stat().AcquiredConns(), expected)
}

func schemasInDifferentBuckets(t *testing.T, permits *Pool) (string, string) {
	t.Helper()
	first := "permit_blocked_0"
	for i := 1; i < 100; i++ {
		candidate := fmt.Sprintf("permit_healthy_%d", i)
		if permits.Bucket(candidate) != permits.Bucket(first) {
			return first, candidate
		}
	}
	t.Fatal("could not find schemas in different buckets")
	return "", ""
}
