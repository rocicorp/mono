//go:build vfs
// +build vfs

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

// openMemDB opens a plain (non-VFS) in-memory sqlite3 db. It exercises the
// same driver vfs-query uses at runtime, without requiring a Litestream
// replica, so it's enough to unit-test runQuery/backupTime's own logic.
func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open mem db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRunQuery_RowsToObjects(t *testing.T) {
	db := openMemDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "CREATE TABLE t (a INTEGER, b TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (1, 'x'), (2, 'y')"); err != nil {
		t.Fatal(err)
	}

	qr, err := runQuery(ctx, db, "SELECT a, b FROM t ORDER BY a", 5*time.Second)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	want := queryResult{
		{"a": int64(1), "b": "x"},
		{"a": int64(2), "b": "y"},
	}
	gotJSON, _ := json.Marshal(qr)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("runQuery rows = %s, want %s", gotJSON, wantJSON)
	}
}

func TestRunQuery_EmptyResultIsEmptyArrayNotNull(t *testing.T) {
	db := openMemDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "CREATE TABLE t (a INTEGER)"); err != nil {
		t.Fatal(err)
	}

	qr, err := runQuery(ctx, db, "SELECT a FROM t", 5*time.Second)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	// A caller consuming query_result as a JSON array must never see `null`
	// for zero rows; that would force every consumer to null-check.
	got, err := json.Marshal(qr)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "[]" {
		t.Errorf("empty query_result marshaled to %s, want []", got)
	}
}

func TestRunQuery_BlobRenderedAsString(t *testing.T) {
	db := openMemDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "CREATE TABLE t (a BLOB)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (x'68656c6c6f')"); err != nil { // "hello"
		t.Fatal(err)
	}

	qr, err := runQuery(ctx, db, "SELECT a FROM t", 5*time.Second)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(qr) != 1 {
		t.Fatalf("got %d rows, want 1", len(qr))
	}
	if v, ok := qr[0]["a"].(string); !ok || v != "hello" {
		t.Errorf("blob column = %#v, want string \"hello\"", qr[0]["a"])
	}
}

func TestBackupTime_NonInitialIsWallClockNow(t *testing.T) {
	db := openMemDB(t)
	logger := slog.New(slog.NewTextHandler(discard{}, nil))

	before := time.Now().UTC()
	got := backupTime(context.Background(), db, false /* first */, 5*time.Second, logger)
	after := time.Now().UTC()

	ts, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Fatalf("backupTime returned unparseable time %q: %v", got, err)
	}
	if ts.Before(before) || ts.After(after) {
		t.Errorf("backupTime = %s, want between %s and %s", ts, before, after)
	}
}

func TestBackupTime_FirstQueryFailureReturnsEmpty(t *testing.T) {
	db := openMemDB(t)
	logger := slog.New(slog.NewTextHandler(discard{}, nil))

	// A plain (non-VFS) connection doesn't understand `PRAGMA litestream_time`;
	// backupTime should degrade to "" rather than panicking or blocking.
	got := backupTime(context.Background(), db, true /* first */, 5*time.Second, logger)
	if got != "" {
		t.Errorf("backupTime on non-VFS connection = %q, want \"\"", got)
	}
}

func TestStaleLag(t *testing.T) {
	const maxLag = 60 * time.Second
	cases := []struct {
		name string
		lag  int64
		want bool
	}{
		{"never polled is not stale", -1, false},
		{"zero is fresh", 0, false},
		{"below threshold", 59, false},
		{"at threshold is not stale", 60, false}, // strict >
		{"just over threshold", 61, true},
		{"far over threshold", 600, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := staleLag(c.lag, maxLag); got != c.want {
				t.Errorf("staleLag(%d, %s) = %v, want %v", c.lag, maxLag, got, c.want)
			}
		})
	}
}

func TestPollLagSeconds_UnsupportedPragmaErrors(t *testing.T) {
	db := openMemDB(t)

	// A plain (non-VFS) connection doesn't understand `PRAGMA litestream_lag`
	// (it returns no rows), so pollLagSeconds should surface an error rather
	// than a bogus lag reading — the caller treats that as "can't tell", not
	// as a wedge.
	if _, err := pollLagSeconds(context.Background(), db, 5*time.Second); err == nil {
		t.Error("pollLagSeconds on non-VFS connection = nil error, want error")
	}
}

func TestNewLogger_LevelMapping(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
		"":      slog.LevelWarn, // unrecognized -> default
		"bogus": slog.LevelWarn,
		"DEBUG": slog.LevelDebug, // case-insensitive
	}
	for in, want := range cases {
		logger := newLogger(in, "json")
		if !logger.Enabled(context.Background(), want) {
			t.Errorf("newLogger(%q): expected level %v to be enabled", in, want)
		}
		if want != slog.LevelDebug && logger.Enabled(context.Background(), want-1) {
			t.Errorf("newLogger(%q): level below %v should not be enabled", in, want)
		}
	}
}

func TestFatalSQLiteErr_Nil(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(discard{}, nil))
	if err := fatalSQLiteErr(nil, logger); err != nil {
		t.Errorf("fatalSQLiteErr(nil) = %v, want nil", err)
	}
}

func TestFatalSQLiteErr_NonSQLiteErrorIsNotFatal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(discard{}, nil))
	// Not a sqlite3.Error at all (e.g. a context timeout) — errors.As won't
	// match, so this should fall through as non-fatal rather than panic.
	if err := fatalSQLiteErr(errors.New("boom"), logger); err != nil {
		t.Errorf("fatalSQLiteErr(non-sqlite err) = %v, want nil", err)
	}
}

func TestFatalSQLiteErr_TransientIsNotFatal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(discard{}, nil))
	for _, code := range []sqlite3.ErrNo{sqlite3.ErrBusy, sqlite3.ErrLocked} {
		sqliteErr := sqlite3.Error{Code: code}
		if err := fatalSQLiteErr(sqliteErr, logger); err != nil {
			t.Errorf("fatalSQLiteErr(code=%d) = %v, want nil (should retry, not abort)", code, err)
		}
	}
}

func TestFatalSQLiteErr_UnrecognizedCodeIsNotFatal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(discard{}, nil))
	// e.g. ErrIoErr: a transient VFS/disk read failure should be retried,
	// not treated as a code bug requiring the process to exit.
	sqliteErr := sqlite3.Error{Code: sqlite3.ErrIoErr}
	if err := fatalSQLiteErr(sqliteErr, logger); err != nil {
		t.Errorf("fatalSQLiteErr(ErrIoErr) = %v, want nil", err)
	}
}

func TestFatalSQLiteErr_FatalCodesReturnError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(discard{}, nil))
	for _, code := range []sqlite3.ErrNo{sqlite3.ErrError, sqlite3.ErrConstraint} {
		sqliteErr := sqlite3.Error{Code: code}
		err := fatalSQLiteErr(sqliteErr, logger)
		if err == nil {
			t.Fatalf("fatalSQLiteErr(code=%d) = nil, want a fatal error", code)
		}
		if !errors.Is(err, sqliteErr) {
			t.Errorf("fatalSQLiteErr(code=%d) = %v, want it to wrap the original sqlite3.Error", code, err)
		}
	}
}

func TestFatalSQLiteErr_DetectsWrappedError(t *testing.T) {
	// runQuery's error may itself be wrapped (e.g. by db/sql or a future
	// fmt.Errorf("%w", ...) call site) before it reaches fatalSQLiteErr;
	// errors.As must still find the sqlite3.Error underneath.
	logger := slog.New(slog.NewTextHandler(discard{}, nil))
	wrapped := fmt.Errorf("query failed: %w", sqlite3.Error{Code: sqlite3.ErrError})
	if err := fatalSQLiteErr(wrapped, logger); err == nil {
		t.Error("fatalSQLiteErr(wrapped fatal error) = nil, want a fatal error")
	}
}

// discard is an io.Writer that drops everything, so tests don't spam stderr.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
