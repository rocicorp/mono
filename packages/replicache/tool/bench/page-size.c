/**
 * How `PRAGMA page_size` behaves for a `WITHOUT ROWID` key/value table.
 *
 * `entry` is `WITHOUT ROWID` (see `src/kv/sqlite-store.ts`), which makes it an
 * index B-tree. SQLite caps an index B-tree's inline payload at
 * `((page_size - 35) * 64 / 255) - 23` — about 1004 bytes at 4096 and 2029 at
 * 8192 — and spills anything larger into an overflow page chain.
 *
 * Read the `cliff` and `page-size` sections for what that costs, but do not
 * read them as this store's workload. Replicache's `entry` rows are B-tree
 * chunks, which `BTreeWrite` targets at 8-16KB (`src/btree/write.ts`), so they
 * overflow at 4096 and 8192 alike; 8192 halves the number of overflow pages a
 * chunk spans rather than avoiding the spill. The 4.4x file-size cliff those
 * sections show belongs to ~1KB rows, which this store does not normally hold.
 * The `chunks` section measures the sizes it does hold. For the numbers that
 * actually justify the default, see `src/kv/sqlite-store.ts` — they are from
 * `replicache-perf/rn` on real devices, not from here.
 *
 * This measures the effect directly, so the claim in `sqlite-store.ts` is
 * reproducible rather than folklore. It links the SQLite amalgamation, so it
 * isolates storage-engine behavior from anything React Native, JSI or JS adds
 * on top — which also means the absolute numbers are desktop numbers. Ratios
 * are the point; for on-device timings use `packages/replicache-perf/rn`.
 *
 * How faithful each workload is to the store:
 *
 * - schema and pragmas: identical, including the order they are issued in.
 * - `get`: identical — `SELECT value FROM entry WHERE key = ?`.
 * - bulk write: the store's `putN(128)` statement, the widest `MAX_BATCH`
 *   width `execInBatches` uses, inside one transaction. It skips the JS above
 *   it (`WriteImplBase`'s pending map, `JSON.stringify`, the power-of-two
 *   split across widths), so it is the store's SQL rather than the store's
 *   write path.
 * - `scan(100)`: NOT a store statement. `SQLiteStore` has no range scan at
 *   all; Replicache walks key ranges above the kv layer. It is here as a probe
 *   of B-tree locality, because overflow chains hurt sequential access more
 *   than point lookups, and that is worth seeing. Do not read it as a store
 *   operation.
 *
 * Build and run (needs a C compiler and a sqlite3 amalgamation):
 *
 *     cc -O2 -o page-size page-size.c sqlite3.c -lpthread -ldl -lm
 *     ./page-size            # all sections
 *     ./page-size chunks     # the 8-16KB sizes this store actually holds
 *     ./page-size cliff      # the ~1KB value-size sweep
 *
 * A matching amalgamation ships inside op-sqlite, which is what the store runs
 * against on React Native:
 *
 *     cp node_modules/@op-engineering/op-sqlite/cpp/sqlite3.[ch] .
 *
 * Any reasonably recent sqlite3.c works; the thresholds are a function of
 * page_size, not of the SQLite version.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

#define ROWS 100000
#define MMAP_SIZE "268435456" /* 256MB, same as the store's default */
#define MAX_BATCH 128         /* matches MAX_BATCH in sqlite-store.ts */

static double now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static void ex(sqlite3 *db, const char *sql) {
  char *err = NULL;
  if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
    fprintf(stderr, "%s: %s\n", sql, err);
    exit(1);
  }
}

static sqlite3_int64 pragma_int(sqlite3 *db, const char *pragma) {
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, pragma, -1, &stmt, NULL);
  sqlite3_step(stmt);
  sqlite3_int64 v = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  return v;
}

/** The store's `putN(n)` SQL: INSERT OR REPLACE ... VALUES (?,?),(?,?),... */
static char *put_n_sql(int n) {
  size_t cap = 64 + (size_t)n * 8;
  char *s = malloc(cap);
  strcpy(s, "INSERT OR REPLACE INTO entry (key, value) VALUES ");
  for (int i = 0; i < n; i++) {
    strcat(s, i ? ",(?,?)" : "(?,?)");
  }
  return s;
}

struct result {
  double file_mb;
  double amplification;
  double bulk_ms;
  double get_us;
  double scan_us;
};

/**
 * Populates a fresh database and measures it.
 *
 * `without_rowid` selects the store's layout (1) or a plain rowid table (0),
 * which is what expo-sqlite's own key-value store uses. `mmap` toggles
 * `PRAGMA mmap_size`.
 */
static struct result measure(int value_size, int page_size, int without_rowid,
                             int mmap, int rows) {
  char path[256], wal[300], shm[300], buf[128];
  snprintf(path, sizeof path, "/tmp/rep-bench-%d-%d-%d-%d.db", value_size,
           page_size, without_rowid, mmap);
  snprintf(wal, sizeof wal, "%s-wal", path);
  snprintf(shm, sizeof shm, "%s-shm", path);
  remove(path);
  remove(wal);
  remove(shm);

  sqlite3 *db;
  if (sqlite3_open(path, &db) != SQLITE_OK) {
    fprintf(stderr, "open %s failed\n", path);
    exit(1);
  }

  /* Order matches setupDatabase(): page_size must precede journal_mode or
     SQLite silently ignores it. Flipping these two lines is the whole point
     of the ordering test in sqlite-store.test.node.ts. */
  snprintf(buf, sizeof buf, "PRAGMA page_size = %d", page_size);
  ex(db, buf);
  ex(db, "PRAGMA busy_timeout = 200");
  ex(db, "PRAGMA journal_mode = 'WAL'");
  ex(db, "PRAGMA synchronous = 'NORMAL'");
  if (mmap) {
    ex(db, "PRAGMA mmap_size = " MMAP_SIZE);
  }

  ex(db, without_rowid
             ? "CREATE TABLE entry (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
               " WITHOUT ROWID"
             : "CREATE TABLE entry (key TEXT PRIMARY KEY, value TEXT NOT NULL)");

  char *value = malloc(value_size + 1);
  memset(value, 'x', value_size);
  value[value_size] = '\0';
  char key[64];

  /* Bulk load through the store's own putN(MAX_BATCH) statement, inside one
     transaction, as a persist does. The statement binds a fixed MAX_BATCH
     rows, so round down rather than overshooting into a partial batch; the
     store handles a remainder by stepping down through narrower widths, which
     is not what this is measuring. */
  const int written = (rows / MAX_BATCH) * MAX_BATCH;
  char *put_sql = put_n_sql(MAX_BATCH);
  ex(db, "BEGIN");
  sqlite3_stmt *ins;
  if (sqlite3_prepare_v2(db, put_sql, -1, &ins, NULL) != SQLITE_OK) {
    fprintf(stderr, "prepare putN: %s\n", sqlite3_errmsg(db));
    exit(1);
  }
  double t0 = now_us();
  for (int i = 0; i < written; i += MAX_BATCH) {
    for (int j = 0; j < MAX_BATCH; j++) {
      snprintf(key, sizeof key, "c/%08x/key-with-realistic-length", i + j);
      sqlite3_bind_text(ins, j * 2 + 1, key, -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(ins, j * 2 + 2, value, value_size, SQLITE_STATIC);
    }
    sqlite3_step(ins);
    sqlite3_reset(ins);
  }
  double bulk_ms = (now_us() - t0) / 1000.0;
  sqlite3_finalize(ins);
  free(put_sql);
  ex(db, "COMMIT");
  ex(db, "PRAGMA wal_checkpoint(TRUNCATE)");

  double file_mb =
      (double)(pragma_int(db, "PRAGMA page_count") *
               pragma_int(db, "PRAGMA page_size")) / 1048576.0;
  /* Key plus value plus a few bytes of row header; close enough to call the
     ratio a storage amplification rather than a precise overhead. */
  double logical_mb = (double)written * (value_size + 38) / 1048576.0;

  /* Random point reads, prepared once — the store's get() path. */
  sqlite3_stmt *get;
  sqlite3_prepare_v2(db, "SELECT value FROM entry WHERE key = ?", -1, &get,
                     NULL);
  const int reads = 30000;
  t0 = now_us();
  for (int i = 0; i < reads; i++) {
    snprintf(key, sizeof key, "c/%08x/key-with-realistic-length",
             (i * 7919) % written);
    sqlite3_bind_text(get, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_step(get);
    sqlite3_column_text(get, 0);
    sqlite3_reset(get);
  }
  double get_us = (now_us() - t0) / reads;
  sqlite3_finalize(get);

  /* Forward range scan. NOT a store statement — SQLiteStore has no range
     scan; Replicache walks key ranges above the kv layer. It is here because
     overflow chains penalize sequential access more than point lookups, which
     the get() column alone would hide. */
  sqlite3_stmt *scan;
  sqlite3_prepare_v2(db,
                     "SELECT key, value FROM entry WHERE key >= ?"
                     " ORDER BY key LIMIT 100",
                     -1, &scan, NULL);
  const int scans = 2000;
  t0 = now_us();
  for (int i = 0; i < scans; i++) {
    snprintf(key, sizeof key, "c/%08x/key-with-realistic-length",
             (i * 7919) % written);
    sqlite3_bind_text(scan, 1, key, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(scan) == SQLITE_ROW) {
      sqlite3_column_text(scan, 1);
    }
    sqlite3_reset(scan);
  }
  double scan_us = (now_us() - t0) / scans;
  sqlite3_finalize(scan);

  sqlite3_close(db);
  remove(path);
  remove(wal);
  remove(shm);
  free(value);

  struct result r = {file_mb, file_mb / logical_mb, bulk_ms, get_us, scan_us};
  return r;
}

static void row(const char *label, struct result r) {
  printf("%-38s | %7.1f MB %5.2fx | %8.1f ms | %7.2f us | %8.1f us\n", label,
         r.file_mb, r.amplification, r.bulk_ms, r.get_us, r.scan_us);
}

/* `*` on scan: not a store statement, see the file header. */
static void header(void) {
  printf("%-38s | %-18s | %-11s | %-10s | %-11s\n", "config", "file",
         "bulk write", "get", "scan(100)*");
  printf("---------------------------------------"
         "+--------------------+-------------+------------+------------\n");
}

/** Printed once under each table, so the scan column is not misread. */
static void footnote(void) {
  printf("* scan is not a store statement — SQLiteStore has no range scan.\n"
         "  It is a B-tree locality probe; see the file header.\n");
}

/** page_size sweep at 1KB values: the reason for the default. */
static void section_page_size(void) {
  printf("\n== page_size, WITHOUT ROWID, 1KB values, %d rows ==\n", ROWS);
  header();
  char label[64];
  for (int p = 4096; p <= 32768; p *= 2) {
    snprintf(label, sizeof label, "page_size=%-6d%s", p,
             p == 4096 ? " (SQLite default)" : p == 8192 ? " (store default)" : "");
    row(label, measure(1024, p, 1, 1, ROWS));
  }
  footnote();
}

/** WITHOUT ROWID vs a plain rowid table, which is what expo-sqlite's kv uses. */
static void section_layout(void) {
  printf("\n== table layout, 1KB values, %d rows ==\n", ROWS);
  header();
  row("WITHOUT ROWID  page_size=4096", measure(1024, 4096, 1, 1, ROWS));
  row("WITHOUT ROWID  page_size=8192", measure(1024, 8192, 1, 1, ROWS));
  row("rowid table    page_size=4096", measure(1024, 4096, 0, 1, ROWS));
  row("rowid table    page_size=8192", measure(1024, 8192, 0, 1, ROWS));
  printf("\nA rowid table keeps values off the index B-tree, so it never hits\n"
         "the overflow cliff and is far less sensitive to page_size. It is\n"
         "still slower than WITHOUT ROWID once page_size is set correctly.\n");
  footnote();
}

/** What mmap_size is worth on top of a correct page_size. */
static void section_mmap(void) {
  printf("\n== mmap_size, WITHOUT ROWID, 1KB values, %d rows ==\n", ROWS);
  header();
  row("page_size=4096  mmap off", measure(1024, 4096, 1, 0, ROWS));
  row("page_size=4096  mmap 256MB", measure(1024, 4096, 1, 1, ROWS));
  row("page_size=8192  mmap off", measure(1024, 8192, 1, 0, ROWS));
  row("page_size=8192  mmap 256MB", measure(1024, 8192, 1, 1, ROWS));
  footnote();
}

/**
 * Locates the overflow cliff for each page size. The jump is abrupt and lands
 * where `((page_size - 35) * 64 / 255) - 23` predicts: ~1004 bytes at 4096,
 * ~2029 at 8192, ~4081 at 16384.
 */
static void section_cliff(void) {
  const int sizes[] = {200, 600, 900, 950, 1000, 1050, 1200,
                       1600, 2000, 2100, 2400, 4000, 4200};
  const int n = (int)(sizeof sizes / sizeof *sizes);
  const int cliff_rows = 20000; /* smaller: 13 sizes x 3 page sizes */

  printf("\n== storage amplification by value size (%d rows) ==\n", cliff_rows);
  printf("%8s %10s %10s %10s\n", "value", "pg=4096", "pg=8192", "pg=16384");
  printf("-------- ---------- ---------- ----------\n");
  for (int i = 0; i < n; i++) {
    printf("%7dB %9.2fx %9.2fx %9.2fx\n", sizes[i],
           measure(sizes[i], 4096, 1, 1, cliff_rows).amplification,
           measure(sizes[i], 8192, 1, 1, cliff_rows).amplification,
           measure(sizes[i], 16384, 1, 1, cliff_rows).amplification);
    fflush(stdout);
  }
}

/**
 * The sizes Replicache's `entry` rows actually take: `BTreeWrite` targets
 * 8-16KB chunks. Every one of these overflows at both page sizes, so this shows
 * what page_size is worth once the spill is unavoidable — a much smaller effect
 * than the cliff, and the one that applies to this store.
 */
static void section_chunks(void) {
  const int sizes[] = {8192, 12288, 16384};
  const int chunk_rows = 12800; /* ~100-200MB; a multiple of MAX_BATCH */

  printf("\n== B-tree chunk sizes (8-16KB), WITHOUT ROWID, %d rows ==\n",
         chunk_rows);
  header();
  char label[64];
  for (unsigned i = 0; i < sizeof sizes / sizeof *sizes; i++) {
    for (int p = 4096; p <= 16384; p *= 2) {
      snprintf(label, sizeof label, "value=%5dB  page_size=%-6d", sizes[i], p);
      row(label, measure(sizes[i], p, 1, 1, chunk_rows));
    }
  }
  footnote();
  printf("All of these overflow at every page size shown. page_size only\n"
         "changes how many overflow pages each row is split across.\n");
}

int main(int argc, char **argv) {
  const char *only = argc > 1 ? argv[1] : NULL;
  int all = only == NULL;

  if (all || strcmp(only, "page-size") == 0) section_page_size();
  if (all || strcmp(only, "layout") == 0) section_layout();
  if (all || strcmp(only, "mmap") == 0) section_mmap();
  if (all || strcmp(only, "cliff") == 0) section_cliff();
  if (all || strcmp(only, "chunks") == 0) section_chunks();

  if (!all && strcmp(only, "page-size") && strcmp(only, "layout") &&
      strcmp(only, "mmap") && strcmp(only, "cliff") &&
      strcmp(only, "chunks")) {
    fprintf(stderr, "usage: %s [page-size|layout|mmap|cliff|chunks]\n",
            argv[0]);
    return 2;
  }
  return 0;
}
