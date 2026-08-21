//go:build vfs
// +build vfs

// Command vfs-query polls a Litestream backup through the VFS and streams the
// result of a SQL query to stdout as newline-delimited JSON, emitting a line
// only when the query result changes.
//
// It exists because the Litestream VFS ships as a Go c-archive SQLite loadable
// extension, and loading that via dlopen() after process start hits two
// unfixed upstream Go runtime bugs on musl (golang/go#54805) and arm64
// (golang/go#62556). Running the VFS in a normal Go process — as this program
// does — sidesteps both: the Go runtime is the initial program, so there is no
// late dlopen and no initial-exec TLS relocation.
//
// The caller (a Node poller) spawns this program while the backup is catching
// up to the local replica and SIGTERMs it once they are in sync. Each emitted
// line separates the query result from server metadata to avoid any column /
// field collisions:
//
//	{"query_result":[{"stateVersion":"2z…","writeTimeMs":…}],
//	 "metadata":{"litestream_time":"2026-…Z"}}
//
// metadata.litestream_time is the backup-data time used by the caller for the
// backup_lag metric. On the first emission it's the real `PRAGMA
// litestream_time` (accurate at open); on later emissions it's wall-clock now
// (a stopgap — see backupTime). Logs go to stderr so they don't mingle with the
// JSON stream on stdout.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3" // cgo SQLite that hosts the VFS (driver "sqlite3")

	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	_ "github.com/benbjohnson/litestream/file" // register file:// replica backend
	_ "github.com/benbjohnson/litestream/gs"   // register gs:// replica backend
	_ "github.com/benbjohnson/litestream/s3"   // register s3:// replica backend
)

const vfsName = "litestream"

const maxLocalPollInterval time.Duration = 30 * time.Second

// shutdownGracePeriod bounds how long we wait, after receiving SIGINT/SIGTERM
// or seeing stdin close, for a graceful exit before forcing one. Normally
// ctx cancellation is enough: the poll loop's `select` sees ctx.Done() and
// returns promptly. But litestream's VFS Open() blocks in its own internal
// loop while waiting for the *first* backup to appear (a loop with no
// visibility into our ctx at all), and Go cannot interrupt a call blocked in
// someone else's library — so if that's what we're stuck in, this is the
// only way out. Verified: steady-state polling already exits cleanly on
// signal well within this window; only the pre-first-backup case needs it.
const shutdownGracePeriod = 5 * time.Second

func main() {
	replicaURL := flag.String("replica-url", "", "Litestream replica URL, e.g. s3://bucket/path (required)")
	query := flag.String("query", "", "SQL query to poll and stream on change (required)")
	remotePollInterval := flag.Duration("remote-poll-interval", time.Second, "how often vfs polls the replica")
	localPollInterval := flag.Duration("local-poll-interval", time.Second, "how often to poll the local vfs cache")
	queryTimeout := flag.Duration("query-timeout", 30*time.Second, "timeout for a single query cycle")
	emitMetadata := flag.Bool("emit-metadata", true, "attach metadata (litestream_time) on each emission")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat := flag.String("log-format", "json", "log format: json, text")
	watchStdin := flag.Bool("watch-stdin", true, "exit when stdin closes (parent-death guard); disable for standalone use")
	maxPollLag := flag.Duration("max-poll-lag", 0, "exit non-zero if the VFS hasn't polled the replica successfully within this window (0 = derive from remote-poll-interval); the parent then restarts the process, which recovers a wedged follower")
	flag.Parse()

	logger := newLogger(*logLevel, *logFormat)
	if err := run(*replicaURL, *query, *remotePollInterval, *localPollInterval, *queryTimeout, *maxPollLag, *emitMetadata, *watchStdin, logger); err != nil {
		logger.Error("vfs-query exited with error", "err", err)
		os.Exit(1)
	}
}

func run(replicaURL, query string, remotePollInterval, localPollInterval, queryTimeout, maxPollLag time.Duration, emitMetadata, watchStdin bool, logger *slog.Logger) error {
	if replicaURL == "" {
		return errors.New("--replica-url is required")
	}
	if strings.TrimSpace(query) == "" {
		return errors.New("--query is required")
	}

	// Watchdog for a permanently-wedged follower. The VFS can get stuck such
	// that its background poll loop keeps failing (e.g. litestream's
	// "non-contiguous ltx file" at L1) while queries still succeed against the
	// frozen page index — so nothing surfaces the failure to us. `PRAGMA
	// litestream_lag` reports seconds since the VFS last polled the replica
	// *successfully*: near-zero for a healthy (even idle) follower, and growing
	// without bound only when polls fail. If it exceeds maxPollLag we exit
	// non-zero; the parent poller restarts us, and a fresh open re-seeds the
	// index from real L1 boundaries, which clears the wedge.
	if maxPollLag <= 0 {
		// Default to missing 2 poll intervals, or at least 20 seconds.
		maxPollLag = min(20*time.Second, (2*remotePollInterval)+localPollInterval)
	}

	// Build the replica client and register the VFS. The client is bound to the
	// VFS, so the DSN only needs `?vfs=<name>`.
	client, err := litestream.NewReplicaClientFromURL(replicaURL)
	if err != nil {
		return fmt.Errorf("create replica client: %w", err)
	}
	vfs := litestream.NewVFS(client, logger)
	vfs.PollInterval = remotePollInterval
	if err := sqlite3vfs.RegisterVFS(vfsName, vfs); err != nil {
		return fmt.Errorf("register vfs: %w", err)
	}

	// One long-lived connection so the VFS stays open (and keeps polling S3)
	// for the life of the process. Filename is arbitrary; mode=ro: reader.
	db, err := sql.Open("sqlite3", "file:zero-backup.db?vfs="+vfsName+"&mode=ro")
	if err != nil {
		return fmt.Errorf("open vfs db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	defer db.Close()

	// Exit on SIGINT/SIGTERM (the caller's normal "pause" signal) or, if
	// watching stdin, when the parent that holds our stdin dies (EOF) — so we
	// don't linger with open S3 connections after the caller is gone.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		t := time.NewTimer(shutdownGracePeriod)
		defer t.Stop()
		<-t.C
		logger.Warn("graceful shutdown did not complete in time; forcing exit", "grace", shutdownGracePeriod)
		os.Exit(0)
	}()
	if watchStdin {
		go func() {
			_, _ = io.Copy(io.Discard, os.Stdin)
			logger.Info("stdin closed; shutting down")
			stop()
		}()
	}

	logger.Info("vfs-query started", "replica", replicaURL, "remotePollInterval", remotePollInterval)

	var last string // last emitted query result (dedup key: the query result only)
	var emitted bool
	var pollInterval = localPollInterval
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		qr, err := runQuery(ctx, db, query, queryTimeout)
		if err != nil {
			if ferr := fatalSQLiteErr(err, logger); ferr != nil {
				return ferr
			}
			pollInterval = min(pollInterval*2, maxLocalPollInterval)
			logger.Warn("query failed; will retry", "err", err, "backoff", pollInterval)
		} else {
			pollInterval = localPollInterval
			// Dedup on the query result only; metadata (litestream_time) rides
			// along but doesn't drive emissions (the caller derives lag from
			// now - litestream_time).
			key, _ := json.Marshal(qr)
			if string(key) != last {
				last = string(key)
				emit(ctx, db, qr, emitMetadata, !emitted, queryTimeout, logger)
				emitted = true
			}
		}

		// Bail out if the VFS poll loop has been failing long enough that we're
		// serving stale data with no prospect of self-recovery. A read error
		// here (e.g. ctx canceled during shutdown, or a backend that doesn't
		// support the pragma) is not itself a wedge signal, so we only log it.
		if lag, lerr := pollLagSeconds(ctx, db, queryTimeout); lerr != nil {
			logger.Debug("could not read litestream_lag", "err", lerr)
		} else if staleLag(lag, maxPollLag) {
			return fmt.Errorf("vfs poll stale: %ds since last successful replica poll exceeds %s; exiting for restart", lag, maxPollLag)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(pollInterval):
		}
	}
}

// queryResult is the query's rows as JSON objects (column name -> value). This
// is redundant vs a columns+rows shape, but simplest for the caller to consume
// — especially for the current single-row watermark query.
type queryResult []map[string]any

type output struct {
	QueryResult queryResult `json:"query_result"`
	Metadata    metadata    `json:"metadata"`
}

type metadata struct {
	LitestreamTime string `json:"litestream_time,omitempty"`
}

func emit(ctx context.Context, db *sql.DB, qr queryResult, emitMetadata, first bool, timeout time.Duration, logger *slog.Logger) {
	out := output{QueryResult: qr}

	if emitMetadata {
		out.Metadata.LitestreamTime = backupTime(ctx, db, first, timeout, logger)
	}

	line, err := json.Marshal(out)
	if err != nil {
		logger.Warn("marshal failed", "err", err)
		return
	}
	fmt.Println(string(line)) // single line + newline to stdout
}

// backupTime returns the value for metadata.litestream_time.
//
// On the FIRST emission it reads the real `PRAGMA litestream_time` — which is
// accurate at connection open and matters because the backup may be genuinely
// behind when this process is spawned (that's why it was spawned).
//
// On every subsequent emission it returns wall-clock now. This is a deliberate
// STOPGAP: a non-initial emission only fires because the backup just advanced,
// so the freshest backup time is ~now (within a poll interval). It avoids
// `PRAGMA litestream_time = LATEST`, which would force a page-index rebuild +
// cache purge (real S3 cost) on every emission. Remove once the litestream VFS
// advances latestLTXTime on poll (branch fix-vfs-litestream-time); then read
// the real value every time.
//
// TODO: Remove when https://github.com/benbjohnson/litestream/pull/1455 is in.
func backupTime(ctx context.Context, db *sql.DB, first bool, timeout time.Duration, logger *slog.Logger) string {
	if !first {
		return time.Now().UTC().Format(time.RFC3339Nano)
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var t string
	if err := db.QueryRowContext(qctx, "PRAGMA litestream_time").Scan(&t); err != nil {
		logger.Warn("could not read litestream_time", "err", err)
		return ""
	}
	return t
}

// pollLagSeconds reads `PRAGMA litestream_lag`: seconds since the VFS last
// successfully polled the replica, or -1 if it has never polled successfully.
// It's an in-memory read on the VFS (no S3 round trip), cheap to call each
// cycle. Returns an error if the pragma can't be read (e.g. a non-VFS backend).
func pollLagSeconds(ctx context.Context, db *sql.DB, timeout time.Duration) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lag int64
	if err := db.QueryRowContext(qctx, "PRAGMA litestream_lag").Scan(&lag); err != nil {
		return 0, err
	}
	return lag, nil
}

// staleLag reports whether a litestream_lag reading (in seconds) indicates the
// VFS poll loop is wedged past maxLag. A negative reading (-1 = never polled
// successfully) is not treated as stale, so cold starts before the first backup
// don't trip the watchdog.
func staleLag(lagSeconds int64, maxLag time.Duration) bool {
	if lagSeconds < 0 {
		return false
	}
	return time.Duration(lagSeconds)*time.Second > maxLag
}

func runQuery(ctx context.Context, db *sql.DB, query string, timeout time.Duration) (queryResult, error) {
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rows, err := db.QueryContext(qctx, query)
	if err != nil {
		return queryResult{}, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return queryResult{}, err
	}
	qr := queryResult{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return queryResult{}, err
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok { // render TEXT/BLOB as string
				row[c] = string(b)
			} else {
				row[c] = vals[i]
			}
		}
		qr = append(qr, row)
	}
	return qr, rows.Err()
}

// Configures logging to match the format of the Node logger
func newLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelWarn
	}
	logOptions := slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.MessageKey:
				a.Key = "message"
			case slog.TimeKey:
				a.Key = "timestamp"
			}
			return a
		},
	}
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, &logOptions)
	} else {
		handler = slog.NewTextHandler(os.Stderr, &logOptions)
	}
	return slog.New(handler.WithAttrs(
		[]slog.Attr{{Key: "worker", Value: slog.StringValue("vfs-query")}}),
	)
}

func fatalSQLiteErr(err error, logger *slog.Logger) error {
	if err == nil {
		return nil
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code {
		// Handle Transient Errors
		case sqlite3.ErrBusy, sqlite3.ErrLocked:
			logger.Warn("transient sqlite error; safe to retry with backoff", "err", sqliteErr)

		// Handle Fatal Errors
		case sqlite3.ErrError:
			return fmt.Errorf("fatal sqlite error (syntax or query error): %w", sqliteErr)

		case sqlite3.ErrConstraint:
			return fmt.Errorf("fatal sqlite error (data violates schema constraints): %w", sqliteErr)

		default:
			logger.Warn("sqlite error", "err", sqliteErr, "code", sqliteErr.Code)
		}
	}
	return nil
}
