# Benchmarks

`Sources/ZeroBenchmarks` has ports of the JS client's benchmarks, under the
same names, and benchmarks of the Swift client's own:

```sh
cd swift
swift run -c release ZeroBenchmarks                 # everything, about a minute
swift run -c release ZeroBenchmarks hydration "pokes > put 100" --time 3 --json out.json
```

A filter is a substring of `group > benchmark`. Each benchmark runs for
`--time` seconds (default 1) after a warm-up and reports the median time per
call with the 75th and 99th percentiles, as `packages/shared/src/bench.ts`
does.

The JS sides:

| Swift group                           | JS                                                                                                |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `zero.bench.ts`                       | `pnpm --filter zero-client bench`                                                                 |
| `btree-set.bench.ts`, `hash.bench.ts` | `pnpm --filter shared bench --project shared/bench/node src/btree-set.bench.ts src/hash.bench.ts` |
| `memory`, `hydration`, `mutations`    | `pnpm --filter zero-conformance bench` (`src/bench/client.bench.ts`)                              |
| `pokes`, the round trip               | none: the JS client applies pokes on the next frame, so a JS timing measures the scheduling       |

Without `--project`, the shared benchmarks also run in headless Chromium at
the same time, which slows the Node run by up to 10%.

The benchmarks don't run in CI; numbers from shared runners vary too much to
gate on. CI builds them, so they keep compiling.

## Results

Medians; lower is better. The iPhone is where the client ships, so its
numbers come first. In short: on the iPhone, Swift is ahead of JS in
WebKit on the read path (zero.bench, materializing), optimistic updates,
memory, hashing, and B-tree scans. JS is still ahead on `pk = N`, which is
mostly the cost of setting up a query's pipeline, and on B-tree lookups.
Linux, where JS is slowest, flatters Swift.

Cells marked † are from before the last performance pass (see What they
found) and weren't measured again.

### iPhone

iPhone 15 Pro, iOS 26.7.1, on power, thermal state nominal throughout.
Swift ran in a Release build of an app wrapping `ZeroBenchmarks`; JS ran in
a `WKWebView` (JIT on) in the same app, on esbuild bundles of the same
`.bench.ts` files. The phone slows down under sustained load before its
thermal state shows it, so each suite ran as its own launch after a
three-minute cool-down, in two rounds that swapped which language went
first; each cell is the mean of the two rounds. Swift's rounds agreed within
3%. JS's materialize rows varied by about 30% between rounds, WebKit's timer
has a 20 µs grain, and WebKit gives JS no memory figure.

| Benchmark                                  | JS (WebKit) | Swift    |
| ------------------------------------------ | ----------- | -------- |
| zero.bench: all 1000 rows x 10 columns     | 65.7 µs     | 41.8 µs  |
| zero.bench: pk = N                         | 2.34 µs     | 6.25 µs  |
| zero.bench: lower 500 rows                 | 79.5 µs     | 54.4 µs  |
| materialize 1,000 rows                     | 85 µs       | 41.5 µs  |
| materialize 10,000 rows                    | 807 µs      | 366 µs   |
| materialize 100,000 rows                   | 8.22 ms     | 5.16 ms  |
| memory for 100,000 rows and a view of them | n/a         | 160 MB†  |
| optimistic update with a 1000-row view     | 111 µs      | 12.3 µs† |

| Benchmark                              | JS (WebKit)                | Swift                    |
| -------------------------------------- | -------------------------- | ------------------------ |
| BTreeSet values() full scan (1000)     | 5.56 µs                    | 708 ns                   |
| BTreeSet valuesFrom() from mid         | 2.04 µs                    | 410 ns                   |
| BTreeSet valuesReversed() full scan    | 4.05 µs                    | 575 ns                   |
| BTreeSet valuesFromReversed() from mid | 1.99 µs                    | 274 ns                   |
| BTreeSet has() hit (x3)                | 46 ns                      | 69 ns                    |
| BTreeSet has() miss (x3)               | 21 ns                      | 28 ns                    |
| BTreeSet get() hit (x3)                | 45 ns                      | 71 ns                    |
| BTreeSet add() then delete()           | 85 ns                      | 72 ns                    |
| BTreeSet add() 100 sequential keys     | 4.44 µs                    | 3.72 µs†                 |
| BTreeSet add() 1000 sequential keys    | 52.0 µs                    | 47.1 µs                  |
| BTreeSet sort, then add() 1000 keys    | 98.2 µs                    | 43.9 µs                  |
| h32, 64 / 1024 / 8192 chars            | 156 ns / 1.13 µs / 2.88 µs | 15 ns / 142 ns / 1.10 µs |
| h64, 64 / 1024 / 8192 chars            | 155 ns / 1.19 µs / 3.56 µs | 42 ns / 262 ns / 2.14 µs |
| h128, 64 / 1024 / 8192 chars           | 232 ns / 1.45 µs / 5.74 µs | 47 ns / 539 ns / 4.42 µs |

### Mac and Linux

The Mac is an M2 Max (32 GB) on macOS 26.7 with Swift 6.4 and Node 24.6;
Linux is an 8-core Intel Xeon at 2.6 GHz with Swift 6.2.4 and Node 22.23.
JS ran with the commands above, under vitest. On the Mac, the same bundles
the iPhone ran are 14–40% faster than vitest on the client rows.

| Benchmark                                  | Mac JS     | Mac Swift | Linux JS | Linux Swift |
| ------------------------------------------ | ---------- | --------- | -------- | ----------- |
| zero.bench: all 1000 rows x 10 columns     | 141 µs     | 56.1 µs   | 263 µs   | 70.4 µs     |
| zero.bench: pk = N                         | 5.42 µs    | 6.82 µs   | 7.72 µs  | 8.39 µs     |
| zero.bench: lower 500 rows                 | 169 µs     | 69.6 µs   | 261 µs   | 89.7 µs     |
| materialize 1,000 rows                     | 133 µs     | 53.6 µs   | 282 µs   | 63.2 µs     |
| materialize 10,000 rows                    | 1.27 ms    | 507 µs    | 2.70 ms  | 757 µs      |
| materialize 100,000 rows                   | 16.3 ms    | 6.76 ms   | 43.2 ms  | 7.07 ms     |
| memory for 100,000 rows and a view of them | 240–258 MB | 162 MB†   | 164 MB   | 128 MB†     |
| optimistic update with a 1000-row view     | 120 µs     | 14.7 µs†  | 178 µs   | 23 µs†      |

| Benchmark                           | Mac JS  | Mac Swift | Linux JS | Linux Swift |
| ----------------------------------- | ------- | --------- | -------- | ----------- |
| BTreeSet values() full scan (1000)  | 2.67 µs | 720 ns    | 3.60 µs  | 2.51 µs     |
| BTreeSet has() hit (x3)             | 63 ns   | 86 ns     | 67 ns    | 132 ns      |
| BTreeSet add() 1000 sequential keys | 46.5 µs | 50.3 µs   | 63.6 µs  | 65.3 µs     |
| BTreeSet sort, then add() 1000 keys | 94 µs   | 47.6 µs   | 104 µs   | 38 µs       |
| h64, 1024 chars                     | 5.26 µs | 302 ns    | 5.46 µs  | 318 ns      |

As `hash.bench.ts` notes, vitest keeps imported functions from being
inlined, so its JS hash numbers are pessimistic; bundled, they are a few
times faster.

### Swift only

| Benchmark                                                 | iPhone   | Mac      | Linux    |
| --------------------------------------------------------- | -------- | -------- | -------- |
| materialize 1,000 rows as structs                         | 138 µs   | 148 µs   | 182 µs   |
| materialize 10,000 rows as structs                        | 1.29 ms  | 2.61 ms† | 1.94 ms  |
| materialize 100,000 rows as structs                       | 14.7 ms  | 16.6 ms  | 24.8 ms  |
| optimistic update with a 1000-row live query of structs   | 61 µs†   | 78 µs†   | 91 µs†   |
| poke putting 1 row, 1000-row view                         | 16 µs†   | 19 µs†   | 29 µs†   |
| poke putting 100 rows, 1000-row view                      | 551 µs†  | 695 µs†  | 1.00 ms† |
| poke putting 1000 rows, 1000-row view                     | 5.13 ms† | 6.58 ms† | 9.82 ms† |
| round trip (update, push, confirming poke), 1000-row view | 37 µs†   | 42 µs†   | 80 µs†   |

JS views hand out their rows; Swift apps read them as structs, which costs
about three times the materialization. A live query reads only the rows
that changed after the first read.

## What they found

- The mutation tracker looked mutations up by scanning a list, so each
  mutation cost more than the last while mutations were pending (229 µs on
  average over a one-second run, now 21 µs).
- A live query read every row again on every change (now only the changed
  ones: 92 µs instead of about 640 µs for 1000 rows).
- B-tree nodes paid for dynamic exclusivity checks on every read, and the
  forward iterator read its leaf's keys through the node for every element
  (a full scan of 1000 took 23 µs, now 2.7 µs). Views and sources iterate
  B-trees, so materializing got faster too.
- Hydrating a view took its row list out of the view and put it back for
  every row.
- Comparing strings in UTF-8 order walked them with iterators rather than
  their bytes.
- A row was a `[String: JSONValue]`, so reading a column hashed its name:
  comparing rows by key and reading rows as structs mostly hashed. Rows are
  now keyed by column index (see DESIGN.md). Reading 100,000 rows as structs
  went from 86 to 30 ms, and materializing them from 23 to 12 ms. The cost
  is memory: a source's row no longer shares its storage with the local
  store's copy, which adds about 30 MB per 100,000 rows of 10 columns.
- B-tree lookups retained each node they passed (a lookup took about
  64 ns, now 43 ns).

- zero.bench's Swift check summed column `a` by name on the main actor,
  which on macOS checks its executor for every row: about 45% of "all 1000
  rows" there (243 µs, now 135 µs; 121 µs on the iPhone, now 96 µs). It
  now reads `a` by index, as JS reads `row.a`.

- On Apple platforms, where most of the time went:
  - B-tree nodes held `Array`s, which check for NSArray bridging on every
    read. They now hold `ContiguousArray`s: scans are 3.6–4.5× faster,
    lookups about 2×.
  - A row read its layout through a stored class property, an exclusivity
    check each time.
  - A source's scans were closures over captured variables, each access
    checked, behind an `AnyIterator` that called the B-tree iterator's
    unspecialized `next()`. They're struct iterators now.
  - Hydration inserted each row at the end of the list, which copies it;
    appending moves it.
  - A filter hashed its column's name for every row; it now looks the
    column's index up once per row layout.

  Together, on the iPhone: "all 1000 rows" 95.6 → 41.8 µs, "lower 500"
  115 → 54.4 µs, materializing 1,000 / 10,000 / 100,000 rows 2.0–2.5×
  faster, and 1.6–1.8× as structs. The Mac moved the same way; Linux, where
  `Array` doesn't bridge, gained on the read path but not in the B-tree.

Where Swift is still slower: `pk = N`, whose time is spread over setting up
a query's pipeline with no single hotspot; and B-tree lookups, which call
their comparator through a closure that JS engines inline.
