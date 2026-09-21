# kv storage-engine benchmarks

Standalone C benchmarks for the SQLite schema behind `src/kv/sqlite-store.ts`.

They link the SQLite amalgamation directly, with no React Native, JSI or JS in
the way, so they isolate storage-engine behavior. That makes them good for
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
bytes at 4096. Values past that spill into overflow page chains. Since
`replicache-perf` uses 1KB values, the default page size puts the store just
over the edge:

| page_size | file             | bulk write | get      | scan(100) |
| --------- | ---------------- | ---------- | -------- | --------- |
| 4096      | 446.4 MB (4.41x) | 1406 ms    | 18.68 us | 102.1 us  |
| 8192      | 111.6 MB (1.10x) | 236 ms     | 4.10 us  | 38.1 us   |

The `cliff` section shows how abrupt this is — amplification goes from 1.06x at
950-byte values to 4.62x at 1000-byte values, and the jump reappears at ~2029
bytes for `page_size=8192` and ~4081 for 16384. The cliff never goes away; it
only moves.

File sizes are deterministic and should reproduce exactly. Timings are from one
desktop run and move with machine and load — read them as ratios.

### Two things worth knowing before changing the schema

`page_size` **must** be issued before `journal_mode` and before any table is
created. SQLite ignores it otherwise, silently, with no error — you just end up
back on 4096. `sqlite-store.test.node.ts` asserts the ordering.

Changing `page_size` on a database that already has content requires
`journal_mode=DELETE`, then the pragma, then `VACUUM`, then `journal_mode=WAL`.
The store does not do this, so existing databases keep whatever page size they
were created with.
