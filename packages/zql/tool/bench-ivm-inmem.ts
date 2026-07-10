import {testLogConfig} from '../../../packages/otel/src/test-log-config.ts';
/**
 * In-memory head-to-head bench (Zero ZQL side) vs Jazz and Rindle.
 *
 * A faithful mirror of `crates/jazz-tools/examples/bench_ivm_inmem.rs` (Jazz) and
 * `<rindle>/examples/bench_ivm_inmem.rs` (Rindle): the same three workloads, the
 * same `bench_min` timing harness (~200 ms warmup, min ns/op over 6 rounds x 8
 * samples), and the same machine-line + human-line output format — so the three
 * runs are directly comparable.
 *
 * Measures, for three workloads over a sweep of N rows:
 *   - hydration:   fork the populated source (O(1) COW), build a fresh pipeline +
 *                  ArrayView, hydrate to the first full result (rebuilds the
 *                  secondary sort index — exactly the work a first subscription pays)
 *   - maintenance: apply one source push (Add) that enters the result, propagate to
 *                  the view + flush, then undo it (untimed) to keep N stable
 *   - retained:    heapUsed retained by the materialized view + its secondary index
 *                  after hydration (gc-forced delta; primary data shared/excluded)
 *
 * Leaf: the in-memory `MemorySource` (COW B+tree), same structure Rindle's
 * `MemorySource` ports. Full JS (no native), which is the point of the comparison.
 *
 * Run:   node --experimental-transform-types --expose-gc tool/bench-ivm-inmem.ts
 * Knobs: ZQL_BENCH_SCALES=1000,10000,100000
 */
import {createSilentLogContext} from '../../../packages/shared/src/logging-test-utils.ts';
import type {AST} from '../../../packages/zero-protocol/src/ast.ts';
import type {Row} from '../../../packages/zero-protocol/src/data.ts';
import {buildPipeline} from '../src/builder/builder.ts';
import {TestBuilderDelegate} from '../src/builder/test-builder-delegate.ts';
import {ArrayView} from '../src/ivm/array-view.ts';
import {MemorySource} from '../src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
} from '../src/ivm/source.ts';
import {consume} from '../src/ivm/stream.ts';
import type {Format} from '../src/ivm/view.ts';

const lc = createSilentLogContext();

// ---------------------------------------------------------------------------
// Timing harness: ~200ms warmup, then min ns/op over 6 rounds x 8 samples.
// The closure is self-contained: it does its own untimed state-reset and
// returns only the measured nanoseconds. Mirror of the Rust `bench_min`.
// ---------------------------------------------------------------------------
function benchMin(op: () => number): number {
  const warmEnd = Number(process.hrtime.bigint()) + 200_000_000; // 200ms
  while (Number(process.hrtime.bigint()) < warmEnd) {
    op();
  }
  let best = Infinity;
  for (let round = 0; round < 6; round++) {
    let rb = Infinity;
    for (let s = 0; s < 8; s++) {
      const v = op();
      if (v < rb) rb = v;
    }
    if (rb < best) best = rb;
  }
  return best;
}

// A black-box sink so V8 can't optimize away the work we're measuring.
let sink = 0;
function blackBox(x: number) {
  sink ^= x;
}

// heapUsed after forcing GC. Two passes to settle finalizers.
function memAfterGc(): number {
  const gc = (globalThis as {gc?: () => void}).gc;
  if (gc) {
    gc();
    gc();
  }
  return process.memoryUsage().heapUsed;
}

// ---------------------------------------------------------------------------
// A pre-populated source template. Every build forks it (O(1) COW share of the
// primary btree); the query's sort builds any secondary index lazily during
// hydration — exactly the work a first subscription pays.
// ---------------------------------------------------------------------------
type Src = {
  name: string;
  columns: Record<string, {type: 'number' | 'string'}>;
  pk: [string, ...string[]];
  tmpl: MemorySource;
};

function makeSrc(
  name: string,
  columns: Record<string, {type: 'number' | 'string'}>,
  pk: [string, ...string[]],
  rows: Row[],
): Src {
  const tmpl = new MemorySource(name, columns, pk);
  for (const r of rows) {
    consume(tmpl.push(makeSourceChangeAdd(r)));
  }
  return {name, columns, pk, tmpl};
}

type Built = {
  view: ArrayView<readonly unknown[]>;
  sources: Record<string, MemorySource>;
};

// Fork each source into fresh leaves, build the pipeline, materialize + hydrate
// the ArrayView (its constructor hydrates).
function build(srcs: Src[], ast: AST, format: Format): Built {
  const sources: Record<string, MemorySource> = {};
  for (const s of srcs) {
    sources[s.name] = s.tmpl.fork();
  }
  const delegate = new TestBuilderDelegate(sources);
  const input = buildPipeline(ast, delegate, 'q');
  const view = new ArrayView(input, format, true, () => {}) as ArrayView<
    readonly unknown[]
  >;
  return {view, sources};
}

function rowCount(b: Built): number {
  return (b.view.data as readonly unknown[]).length;
}

// Retained heap per materialized view + its secondary index. heapUsed deltas are
// noisy for small allocations, so build K independent live views, hold them all,
// measure the total delta, and divide. This amplifies the signal well above GC
// jitter. Rows are shared (COW fork) so, like the Rust side, only the view +
// lazily-built secondary index are counted, not the primary data.
function retainedPerView(
  srcs: Src[],
  ast: AST,
  format: Format,
  n: number,
): number {
  const k = n <= 1000 ? 40 : n <= 10000 ? 12 : 4;
  const base = memAfterGc();
  const held: Built[] = [];
  for (let i = 0; i < k; i++) {
    const b = build(srcs, ast, format);
    if (rowCount(b) === 0) throw new Error('hydration empty (retained)');
    held.push(b);
  }
  const total = Math.max(0, memAfterGc() - base);
  blackBox(held.length);
  return total / k;
}

function report(
  workload: string,
  n: number,
  hydNs: number,
  maintNs: number,
  memBytes: number,
) {
  console.log(`zql|${workload}|N=${n}|hydration_ns|${Math.round(hydNs)}`);
  console.log(`zql|${workload}|N=${n}|maintenance_ns|${Math.round(maintNs)}`);
  console.log(`zql|${workload}|N=${n}|retained_bytes|${Math.round(memBytes)}`);
  const pad = (s: string, w: number) => s.padStart(w);
  process.stderr.write(
    `[zql] ${workload.padEnd(15)} N=${pad(String(n), 7)}  ` +
      `hydration=${pad((hydNs / 1000).toFixed(1), 10)}us  ` +
      `maintenance=${pad((maintNs / 1000).toFixed(2), 9)}us  ` +
      `retained=${pad(String(Math.round(memBytes)), 10)} B\n`,
  );
}

// ---------------------------------------------------------------------------
// Workload 1: top-k. item(score) order_by score desc limit 50.
// ---------------------------------------------------------------------------
function workloadTopk(n: number) {
  const rows: Row[] = [];
  for (let i = 0; i < n; i++) rows.push({id: i, score: i, body: 'x'});
  const srcs = [
    makeSrc(
      'item',
      {id: {type: 'number'}, score: {type: 'number'}, body: {type: 'string'}},
      ['id'],
      rows,
    ),
  ];
  const ast: AST = {table: 'item', orderBy: [['score', 'desc']], limit: 50};
  const format: Format = {singular: false, relationships: {}};

  const retained = retainedPerView(srcs, ast, format, n);
  const held = build(srcs, ast, format);
  if (rowCount(held) === 0) throw new Error('topk hydration empty');

  // maintenance: push a new max (enters the top-50 window), then undo (untimed).
  const item = held.sources.item;
  let next = n + 1;
  const maint = benchMin(() => {
    const row = {id: next, score: next, body: 'm'};
    const t = process.hrtime.bigint();
    consume(item.push(makeSourceChangeAdd(row)));
    held.view.flush();
    const ns = Number(process.hrtime.bigint() - t);
    const top = (held.view.data as {score: number}[])[0];
    if (top.score !== next) throw new Error('topk: new max not at top');
    consume(item.push(makeSourceChangeRemove(row)));
    held.view.flush();
    next++;
    return ns;
  });

  // hydration: fork + build + hydrate to first full result each op.
  const hyd = benchMin(() => {
    const t = process.hrtime.bigint();
    const b = build(srcs, ast, format);
    const r = rowCount(b);
    const ns = Number(process.hrtime.bigint() - t);
    if (r === 0) throw new Error('topk hydration empty');
    blackBox(r);
    return ns;
  });

  report('topk', n, hyd, maint, retained);
}

// ---------------------------------------------------------------------------
// Workload 2: filtered top-k.
// item(kind,score) where kind=1 order_by score desc limit 50.
// ---------------------------------------------------------------------------
function workloadFilteredTopk(n: number) {
  const rows: Row[] = [];
  for (let i = 0; i < n; i++)
    rows.push({id: i, kind: i % 2, score: i, body: 'x'});
  const srcs = [
    makeSrc(
      'item',
      {
        id: {type: 'number'},
        kind: {type: 'number'},
        score: {type: 'number'},
        body: {type: 'string'},
      },
      ['id'],
      rows,
    ),
  ];
  const ast: AST = {
    table: 'item',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'kind'},
      op: '=',
      right: {type: 'literal', value: 1},
    },
    orderBy: [['score', 'desc']],
    limit: 50,
  };
  const format: Format = {singular: false, relationships: {}};

  const retained = retainedPerView(srcs, ast, format, n);
  const held = build(srcs, ast, format);
  if (rowCount(held) === 0) throw new Error('filtered_topk hydration empty');

  const item = held.sources.item;
  let next = n + 1;
  const maint = benchMin(() => {
    const row = {id: next, kind: 1, score: next, body: 'm'};
    const t = process.hrtime.bigint();
    consume(item.push(makeSourceChangeAdd(row)));
    held.view.flush();
    const ns = Number(process.hrtime.bigint() - t);
    const top = (held.view.data as {score: number}[])[0];
    if (top.score !== next)
      throw new Error('filtered_topk: new max not at top');
    consume(item.push(makeSourceChangeRemove(row)));
    held.view.flush();
    next++;
    return ns;
  });

  const hyd = benchMin(() => {
    const t = process.hrtime.bigint();
    const b = build(srcs, ast, format);
    const r = rowCount(b);
    const ns = Number(process.hrtime.bigint() - t);
    if (r === 0) throw new Error('filtered_topk hydration empty');
    blackBox(r);
    return ns;
  });

  report('filtered_topk', n, hyd, maint, retained);
}

// ---------------------------------------------------------------------------
// Workload 3: 1:many join. n children joined to ~n/5 parents (child -> parent).
// Top level is the child; each child materializes its one parent. No limit.
// ---------------------------------------------------------------------------
function workloadJoin(n: number) {
  const numParents = Math.max(1, Math.floor(n / 5));
  const parentRows: Row[] = [];
  for (let i = 0; i < numParents; i++) parentRows.push({id: i, name: `p${i}`});
  const childRows: Row[] = [];
  for (let i = 0; i < n; i++)
    childRows.push({id: i, parent_id: i % numParents, body: 'c'});

  const srcs = [
    makeSrc(
      'child',
      {
        id: {type: 'number'},
        parent_id: {type: 'number'},
        body: {type: 'string'},
      },
      ['id'],
      childRows,
    ),
    makeSrc(
      'parent',
      {id: {type: 'number'}, name: {type: 'string'}},
      ['id'],
      parentRows,
    ),
  ];

  // child { parent: parent where parent.id == child.parent_id } — full 1:many
  // join, every child materialized with its parent (no limit).
  const ast: AST = {
    table: 'child',
    related: [
      {
        correlation: {parentField: ['parent_id'], childField: ['id']},
        subquery: {table: 'parent', alias: 'parent'},
      },
    ],
  };
  const format: Format = {
    singular: false,
    relationships: {parent: {singular: false, relationships: {}}},
  };

  const retained = retainedPerView(srcs, ast, format, n);
  const held = build(srcs, ast, format);
  const baseline = rowCount(held);
  if (baseline === 0) throw new Error('join hydration empty');

  const child = held.sources.child;
  let next = n + 1;
  const maint = benchMin(() => {
    const row = {id: next, parent_id: 0, body: 'm'};
    const t = process.hrtime.bigint();
    consume(child.push(makeSourceChangeAdd(row)));
    held.view.flush();
    const ns = Number(process.hrtime.bigint() - t);
    if (rowCount(held) !== baseline + 1)
      throw new Error('join: child not added');
    consume(child.push(makeSourceChangeRemove(row)));
    held.view.flush();
    next++;
    return ns;
  });

  const hyd = benchMin(() => {
    const t = process.hrtime.bigint();
    const b = build(srcs, ast, format);
    const r = rowCount(b);
    const ns = Number(process.hrtime.bigint() - t);
    if (r === 0) throw new Error('join hydration empty');
    blackBox(r);
    return ns;
  });

  report('join', n, hyd, maint, retained);
}

function main() {
  const scales = (process.env.ZQL_BENCH_SCALES ?? '1000,10000,100000')
    .split(',')
    .map(s => Number(s.trim()))
    .filter(x => Number.isFinite(x) && x > 0);

  process.stderr.write('== Zero ZQL in-memory IVM bench (MemorySource) ==\n');
  for (const n of scales) {
    workloadTopk(n);
    workloadFilteredTopk(n);
    workloadJoin(n);
  }
  blackBox(sink);
}

main();
