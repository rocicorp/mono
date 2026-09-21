# kv storage-engine benchmarks

Standalone C benchmarks for the SQLite schema behind `src/kv/sqlite-store.ts`.

They link the SQLite amalgamation directly, with no React Native, JSI or JS in
the way, so they isolate storage-engine behavior. The schema, pragmas and the
`get`/`putN(128)` statements match the store; the `scan` column does not — see
"What each column is" below. That makes them good for
questions like "does this pragma matter" and useless for questions like "how
fast is a `get()` on a Pixel". For the latter, use
`packages/replicache-perf/rn`, which runs the real store on a real device.

## Running

Get a SQLite amalgamation. The one the store actually runs against on React
Native ships inside op-sqlite:

```bash
cp ../../../../node_modules/@op-engineering/op-sqlite/cpp/sqlite3.[ch] .
```

Any reasonably recent `sqlite3.c` works — the thresholds these measure are a
function of `page_size`, not of the SQLite version. Then:

```bash
cc -O2 -o page-size page-size.c sqlite3.c -lpthread -ldl -lm

./page-size             # all sections
./page-size chunks      # the 8-16KB sizes this store actually holds
./page-size page-size   # page_size sweep at 1KB values
./page-size layout      # WITHOUT ROWID vs a plain rowid table
./page-size mmap        # what mmap_size is worth
./page-size cliff       # storage amplification by value size
```

`sqlite3.c`, `sqlite3.h` and the built binary are gitignored.

## page-size.c

Why `setupDatabase()` sets `PRAGMA page_size = 8192` instead of leaving SQLite
on its default of 4096.

`entry` is `WITHOUT ROWID`, so it is an index B-tree, and SQLite caps an index
B-tree's inline payload at `((page_size - 35) * 64 / 255) - 23` — about 1004
bytes at 4096. Values past that spill into overflow page chains. With 1KB rows,
the default page size puts the store just over the edge:

| page_size | file             | bulk write | get      | scan(100) |
| --------- | ---------------- | ---------- | -------- | --------- |
| 4096      | 446.4 MB (4.41x) | 1406 ms    | 18.68 us | 102.1 us  |
| 8192      | 111.6 MB (1.10x) | 236 ms     | 4.10 us  | 38.1 us   |

The `cliff` section shows how abrupt this is — amplification goes from 1.04x at
950-byte values to 4.51x at 1000-byte values, and the jump reappears at ~2029
bytes for `page_size=8192` and ~4081 for 16384. The cliff never goes away; it
only moves.

File sizes are deterministic and should reproduce exactly. Timings are from one
desktop run and move with machine and load — read them as ratios.

This models 1KB rows, not what the store holds in practice. Replicache's kv rows
are B-tree chunks of 8-16KB, which overflow at 4096 and 8192 alike, so the
4.4x → 1.1x above is not the real-world effect of the pragma. For that, use
`replicache-perf/rn` on a device (see the comment on `PAGE_SIZE` in
`src/kv/sqlite-store.ts`).

### Two things worth knowing before changing the schema

`page_size` **must** be issued before `journal_mode` and before any table is
created. SQLite ignores it otherwise, silently, with no error — you just end up
back on 4096. `sqlite-store.test.node.ts` asserts the ordering.

Changing `page_size` on a database that already has content requires
`journal_mode=DELETE`, then the pragma, then `VACUUM`, then `journal_mode=WAL`.
The store does not do this, so existing databases keep whatever page size they
were created with.

### What each column is

| column          | fidelity to the store                                                                                                                                                                                               |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| schema, pragmas | identical, including issue order                                                                                                                                                                                    |
| `get`           | identical — `SELECT value FROM entry WHERE key = ?`                                                                                                                                                                 |
| bulk write      | the store's `putN(128)` SQL in one transaction, but none of the JS above it                                                                                                                                         |
| `scan(100)`     | **not a store statement at all** — `SQLiteStore` has no range scan; Replicache walks ranges above the kv layer. It is a B-tree locality probe, since overflow chains hurt sequential access more than point lookups |

## What this tool does and does not justify

The `cliff` and `page-size` sections use ~1KB rows. **This store does not
normally hold rows that size.** Replicache's `entry` rows are B-tree chunks,
which `BTreeWrite` targets at 8-16KB (`src/btree/write.ts`), so they overflow at
4096 and 8192 alike — 8192 halves the number of overflow pages a chunk spans
rather than avoiding the spill. The 4.4x cliff is real, but it belongs to ~1KB
rows, not to this workload.

The `chunks` section measures the sizes the store does hold, and it is less
flattering to `page_size = 8192` (12800 rows, desktop, file sizes deterministic):

| value | page_size | file                 | bulk write   | get          |
| ----- | --------- | -------------------- | ------------ | ------------ |
| 8 KB  | 4096      | 107.1 MB (1.07x)     | 210.3 ms     | 8.45 us      |
| 8 KB  | **8192**  | **114.3 MB (1.14x)** | **166.8 ms** | **8.67 us**  |
| 12 KB | 4096      | 157.1 MB (1.04x)     | 291.4 ms     | 11.36 us     |
| 12 KB | **8192**  | **214.3 MB (1.42x)** | **305.6 ms** | **11.79 us** |
| 16 KB | 4096      | 207.1 MB (1.03x)     | 381.9 ms     | 14.32 us     |
| 16 KB | **8192**  | **214.3 MB (1.07x)** | **292.1 ms** | **14.81 us** |

At every chunk size 8192 uses _more_ disk than 4096, and point reads are a hair
slower. Bulk writes are mixed — faster at 8KB and 16KB, slower at 12KB. The
waste depends on how the post-inline remainder divides into overflow pages,
which is why 12KB is the worst case at 8192.

None of that settles the question on its own: these are uniform-size rows on a
desktop, real chunks vary across the 8-16KB range, and latency matters more than
bytes on a phone. The numbers that justify the default are the on-device ones in
`src/kv/sqlite-store.ts`. But if you are revisiting `page_size`, start here.
