package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/local/warehousetaskbridge/internal/store"
)

// startPostgres launches a disposable PostgreSQL 16 container and returns a DSN.
// It skips the test with a clear message when Docker is unreachable.
func startPostgres(t *testing.T) string {
	t.Helper()

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("Docker is not reachable (%v); skipping Testcontainers-backed store test", err)
	}
	defer provider.Close()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "bridge",
				"POSTGRES_PASSWORD": "bridge",
				"POSTGRES_DB":       "bridge",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = ctr.Terminate(context.Background())
	})

	endpoint, err := ctr.Endpoint(ctx, "tcp")
	if err != nil {
		t.Fatalf("postgres endpoint: %v", err)
	}
	endpoint = strings.TrimPrefix(endpoint, "tcp://")
	return "postgres://bridge:bridge@" + endpoint + "/bridge?sslmode=disable"
}

func TestMigrateCreatesSchema(t *testing.T) {
	dsn := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	// A second run must be idempotent.
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("second migrate (idempotency): %v", err)
	}

	wantTables := []string{"tasks", "task_events", "bots", "quarantine", "peer_heartbeats", "schema_migrations"}
	for _, table := range wantTables {
		var exists bool
		err := db.Pool().QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = $1)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %q was not created", table)
		}
	}

	var uniqueIdx int
	err = db.Pool().QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE schemaname = 'public' AND tablename = 'tasks'
		  AND indexdef ILIKE '%UNIQUE%' AND indexdef ILIKE '%idempotency_key%'`).Scan(&uniqueIdx)
	if err != nil {
		t.Fatalf("inspect indexes: %v", err)
	}
	if uniqueIdx != 1 {
		t.Fatalf("expected exactly one unique index on tasks.idempotency_key, got %d", uniqueIdx)
	}

	// Prove the constraint is enforced, not merely present.
	_, err = db.Pool().Exec(ctx, `
		INSERT INTO tasks (id, order_ref, idempotency_key, payload_hash, task_type, priority,
			zone, source_slot, dest_slot, quantity, state, created_at, updated_at)
		VALUES (gen_random_uuid(), 'o-1', 'dup-key', 'h1', 'PICK', 10, 'A', 'A-1-1', 'B-1-1', 1,
		        'PENDING', now(), now()),
		       (gen_random_uuid(), 'o-2', 'dup-key', 'h2', 'PICK', 10, 'A', 'A-1-1', 'B-1-1', 1,
		        'PENDING', now(), now())`)
	if err == nil {
		t.Fatal("expected duplicate idempotency_key insert to fail, but it succeeded")
	}
}
