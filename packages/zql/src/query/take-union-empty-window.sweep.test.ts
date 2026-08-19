/**
 * Sweep hunting for Take invariant violations -- `Bound should be set`
 * (take.ts:448) and its siblings -- reachable through a
 * UnionFanOut/UnionFanIn (an OR containing a flipped EXISTS) sitting directly
 * beneath a Take. That is the pipeline shape in the prod stack trace:
 *
 *   source -> [Skip] -> UnionFanOut -> [filter | flipped-exists branches] ->
 *   UnionFanIn -> Take
 *
 * Modelled on `chat.listRich`: nullable leading sort key (`lastMessageAt`),
 * secondary `id` sort, a top-level limit, an OR mixing a plain predicate with
 * a `whereExists`, and optionally a `.start()` cursor (which is what puts the
 * Skip below the fan-out -- see builder.ts:324).
 *
 * Opt in with SWEEP=1. Knobs:
 *   SWEEP_LEN, SWEEP_ONLY_LEN, SWEEP_CONTINUE,
 *   SWEEP_SHAPES, SWEEP_SEEDS, SWEEP_DIRS, SWEEP_LIMITS, SWEEP_OPS
 * Run under `zql` (MemorySource) or `zqlite-zql-test` (SQLite TableSource).
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../ivm/source.ts';
import type {Source} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {
  wrapSourcesWithModeYield,
  type YieldMode,
} from '../ivm/test/mode-yield-source.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const chat = table('chat')
  .columns({
    id: string(),
    lastMessageAt: number().optional(),
    mode: string(),
  })
  .primaryKey('id');

const message = table('message')
  .columns({id: string(), chatId: string(), body: string()})
  .primaryKey('id');

const schema = createSchema({
  tables: [chat, message],
  relationships: [
    relationships(chat, ({many}) => ({
      messages: many({
        sourceField: ['id'],
        destField: ['chatId'],
        destSchema: message,
      }),
    })),
  ],
});

const chatSchema = schema.tables.chat;
const messageSchema = schema.tables.message;

function makeSources(): Record<string, Source> {
  return {
    chat: createSource(
      lc,
      testLogConfig,
      'chat',
      chatSchema.columns,
      chatSchema.primaryKey,
    ),
    message: createSource(
      lc,
      testLogConfig,
      'message',
      messageSchema.columns,
      messageSchema.primaryKey,
    ),
  };
}

type Dir = 'asc' | 'desc';
type Shape = {
  name: string;
  build: (limit: number, dir: Dir) => Query<'chat', typeof schema>;
};

const base = (dir: Dir, limit: number) =>
  newQuery(schema, 'chat')
    .orderBy('lastMessageAt', dir)
    .orderBy('id', dir)
    .limit(limit);

const SHAPES: Shape[] = [
  {
    name: 'or(cmp, exists-flip)',
    build: (limit, dir) =>
      base(dir, limit).where(({or, cmp, exists}) =>
        or(
          cmp('mode', '=', 'a'),
          exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
        ),
      ),
  },
  {
    name: 'or(exists-flip, exists-flip)',
    build: (limit, dir) =>
      base(dir, limit).where(({or, exists}) =>
        or(
          exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          exists('messages', q => q.where('body', '=', 'y'), {flip: true}),
        ),
      ),
  },
  {
    name: 'and(cmp, or(cmp, exists-flip))',
    build: (limit, dir) =>
      base(dir, limit).where(({and, or, cmp, exists}) =>
        and(
          cmp('mode', '!=', 'zzz'),
          or(
            cmp('mode', '=', 'a'),
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          ),
        ),
      ),
  },
  {
    name: 'or(cmp-on-sortkey, exists-flip)',
    build: (limit, dir) =>
      base(dir, limit).where(({or, cmp, exists}) =>
        or(
          cmp('lastMessageAt', '>', 100),
          exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
        ),
      ),
  },
  {
    name: 'or(cmp, exists-noflip) [control]',
    build: (limit, dir) =>
      base(dir, limit).where(({or, cmp, exists}) =>
        or(
          cmp('mode', '=', 'a'),
          exists('messages', q => q.where('body', '=', 'x'), {flip: false}),
        ),
      ),
  },
  // ---- .start() cursor variants: these put a Skip BELOW the fan-out
  // (builder.ts:324), the shape #6122 described.
  {
    name: 'start(null-cursor) + or(cmp, exists-flip)',
    build: (limit, dir) =>
      base(dir, limit)
        .start({id: 'c0', lastMessageAt: null})
        .where(({or, cmp, exists}) =>
          or(
            cmp('mode', '=', 'a'),
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          ),
        ),
  },
  {
    name: 'start(value-cursor) + or(cmp, exists-flip)',
    build: (limit, dir) =>
      base(dir, limit)
        .start({id: 'c1', lastMessageAt: 15})
        .where(({or, cmp, exists}) =>
          or(
            cmp('mode', '=', 'a'),
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          ),
        ),
  },
  {
    name: 'start(null-cursor) + or(cmp-on-sortkey, exists-flip)',
    build: (limit, dir) =>
      base(dir, limit)
        .start({id: 'c0', lastMessageAt: null})
        .where(({or, cmp, exists}) =>
          or(
            cmp('lastMessageAt', '>', 100),
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          ),
        ),
  },
];

type Seed = {name: string; chats: Row[]; messages: Row[]};

const SEEDS: Seed[] = [
  {name: 'empty', chats: [], messages: []},
  {
    name: 'all-null-sortkey, none matching',
    chats: [
      {id: 'c1', lastMessageAt: null, mode: 'b'},
      {id: 'c2', lastMessageAt: null, mode: 'b'},
    ],
    messages: [],
  },
  {
    name: 'all-null-sortkey, one matching',
    chats: [
      {id: 'c1', lastMessageAt: null, mode: 'a'},
      {id: 'c2', lastMessageAt: null, mode: 'b'},
    ],
    messages: [],
  },
  {
    name: 'mixed-null-sortkey',
    chats: [
      {id: 'c1', lastMessageAt: null, mode: 'b'},
      {id: 'c2', lastMessageAt: 50, mode: 'b'},
      {id: 'c3', lastMessageAt: 150, mode: 'b'},
    ],
    messages: [],
  },
  {
    name: 'mixed, one matching via exists',
    chats: [
      {id: 'c1', lastMessageAt: null, mode: 'b'},
      {id: 'c2', lastMessageAt: 50, mode: 'b'},
    ],
    messages: [{id: 'm1', chatId: 'c2', body: 'x'}],
  },
  {
    name: 'non-null sortkey, none matching',
    chats: [
      {id: 'c1', lastMessageAt: 10, mode: 'b'},
      {id: 'c2', lastMessageAt: 20, mode: 'b'},
    ],
    messages: [],
  },
  {
    name: 'null-run then values (cursor lands in NULL group)',
    chats: [
      {id: 'c1', lastMessageAt: null, mode: 'b'},
      {id: 'c2', lastMessageAt: null, mode: 'b'},
      {id: 'c3', lastMessageAt: null, mode: 'b'},
      {id: 'c4', lastMessageAt: 120, mode: 'b'},
    ],
    messages: [],
  },
];

type Op = {name: string; run: (r: Runner) => void};

const OPS: Op[] = [
  {name: 'c1.lma null->200', run: r => r.editChat('c1', {lastMessageAt: 200})},
  {name: 'c1.lma ->null', run: r => r.editChat('c1', {lastMessageAt: null})},
  {name: 'c1.lma ->75', run: r => r.editChat('c1', {lastMessageAt: 75})},
  {name: 'c2.lma ->200', run: r => r.editChat('c2', {lastMessageAt: 200})},
  {name: 'c2.lma ->null', run: r => r.editChat('c2', {lastMessageAt: null})},
  {name: 'c1.mode ->a', run: r => r.editChat('c1', {mode: 'a'})},
  {name: 'c1.mode ->b', run: r => r.editChat('c1', {mode: 'b'})},
  {name: 'c2.mode ->a', run: r => r.editChat('c2', {mode: 'a'})},
  {name: 'c2.mode ->b', run: r => r.editChat('c2', {mode: 'b'})},
  {name: '+msg c1 x', run: r => r.addMessage('mx1', 'c1', 'x')},
  {name: '-msg c1 x', run: r => r.removeMessage('mx1')},
  {name: '+msg c2 x', run: r => r.addMessage('mx2', 'c2', 'x')},
  {name: '-msg c2 x', run: r => r.removeMessage('mx2')},
  {name: 'msg mx1 x->y', run: r => r.editMessage('mx1', {body: 'y'})},
  {
    name: '+chat c9 (null,a)',
    run: r => r.addChat({id: 'c9', lastMessageAt: null, mode: 'a'}),
  },
  {name: '-chat c1', run: r => r.removeChat('c1')},
  {name: '-chat c2', run: r => r.removeChat('c2')},
  {name: 'c3.lma ->null', run: r => r.editChat('c3', {lastMessageAt: null})},
  {name: 'c3.lma ->90', run: r => r.editChat('c3', {lastMessageAt: 90})},
  {name: '+msg c3 x', run: r => r.addMessage('mx3', 'c3', 'x')},
  {name: '-msg c3 x', run: r => r.removeMessage('mx3')},
];

class Runner {
  readonly #sources: Record<string, Source>;
  readonly #chats = new Map<string, Row>();
  readonly #messages = new Map<string, Row>();

  constructor(sources: Record<string, Source>) {
    this.#sources = sources;
  }

  addChat(row: Row) {
    if (this.#chats.has(row.id as string)) return;
    this.#chats.set(row.id as string, row);
    consume(must(this.#sources.chat).push(makeSourceChangeAdd(row)));
  }
  removeChat(id: string) {
    const row = this.#chats.get(id);
    if (!row) return;
    this.#chats.delete(id);
    consume(must(this.#sources.chat).push(makeSourceChangeRemove(row)));
  }
  editChat(id: string, patch: Partial<Row>) {
    const old = this.#chats.get(id);
    if (!old) return;
    const next = {...old, ...patch};
    if (next.lastMessageAt === old.lastMessageAt && next.mode === old.mode) {
      return;
    }
    this.#chats.set(id, next);
    consume(must(this.#sources.chat).push(makeSourceChangeEdit(next, old)));
  }
  addMessage(id: string, chatId: string, body: string) {
    if (this.#messages.has(id)) return;
    const row = {id, chatId, body};
    this.#messages.set(id, row);
    consume(must(this.#sources.message).push(makeSourceChangeAdd(row)));
  }
  removeMessage(id: string) {
    const row = this.#messages.get(id);
    if (!row) return;
    this.#messages.delete(id);
    consume(must(this.#sources.message).push(makeSourceChangeRemove(row)));
  }
  editMessage(id: string, patch: Partial<Row>) {
    const old = this.#messages.get(id);
    if (!old) return;
    const next = {...old, ...patch};
    if (next.body === old.body) return;
    this.#messages.set(id, next);
    consume(must(this.#sources.message).push(makeSourceChangeEdit(next, old)));
  }
}

type Failure = {
  shape: string;
  seed: string;
  dir: Dir;
  limit: number;
  script: string[];
  error: string;
  yieldSeed?: number | undefined;
  stack?: string | undefined;
};

/** Deterministic RNG so any yield-mode failure is replayable from its seed. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const SCRIPT_LEN = Number(process.env.SWEEP_LEN ?? 3);

function pick<T>(all: T[], env: string | undefined): T[] {
  if (!env) return all;
  return env.split(',').map(i => all[Number(i)]);
}

const ACTIVE_OPS = pick(OPS, process.env.SWEEP_OPS);
const ACTIVE_SHAPES = pick(SHAPES, process.env.SWEEP_SHAPES);
const ACTIVE_SEEDS = pick(SEEDS, process.env.SWEEP_SEEDS);
const ACTIVE_DIRS = pick(['desc', 'asc'] as Dir[], process.env.SWEEP_DIRS);
const ACTIVE_LIMITS = pick([1, 2, 3], process.env.SWEEP_LIMITS);
const ONLY_LEN = process.env.SWEEP_ONLY_LEN === '1';
const CONTINUE = process.env.SWEEP_CONTINUE === '1';
// Cooperative-multitasking dimension: prod's view-syncer builds TableSource
// with a shouldYield callback, so 'yield' sentinels thread through every
// fetch/push stream. Nothing in a plain harness ever yields.
const YIELD_SEEDS = Number(process.env.SWEEP_YIELD ?? 0);
// 'fetch' | 'push' | 'both' -- isolates which stream the corruption needs.
const YIELD_MODE = (process.env.SWEEP_YIELD_MODE ?? 'both') as YieldMode;

function* scripts(len: number): Generator<number[]> {
  const start = ONLY_LEN ? len : 1;
  for (let l = start; l <= len; l++) {
    const cur = new Array(l).fill(0);
    for (;;) {
      yield cur.slice();
      let i = l - 1;
      for (; i >= 0; i--) {
        cur[i]++;
        if (cur[i] < ACTIVE_OPS.length) break;
        cur[i] = 0;
      }
      if (i < 0) break;
    }
  }
}

function runOne(
  shape: Shape,
  seed: Seed,
  dir: Dir,
  limit: number,
  script: number[],
  onFailure?: ((f: Failure) => void) | undefined,
  yieldSeed?: number | undefined,
): void {
  let sources = makeSources();
  if (yieldSeed !== undefined) {
    sources = wrapSourcesWithModeYield(
      sources,
      mulberry32(yieldSeed),
      YIELD_MODE,
      0.3,
    );
  }
  const runner = new Runner(sources);
  for (const c of seed.chats) runner.addChat(c);
  for (const m of seed.messages) {
    runner.addMessage(m.id as string, m.chatId as string, m.body as string);
  }

  const delegate = new QueryDelegateImpl({sources});
  const mk = (e: unknown, upTo: number): Failure => ({
    shape: shape.name,
    seed: seed.name,
    dir,
    limit,
    script: script.slice(0, upTo + 1).map(i => ACTIVE_OPS[i].name),
    error: e instanceof Error ? e.message : String(e),
    yieldSeed,
    stack: e instanceof Error ? e.stack : undefined,
  });

  try {
    const view = delegate.materialize(shape.build(limit, dir));
    for (let n = 0; n < script.length; n++) {
      try {
        ACTIVE_OPS[script[n]].run(runner);
      } catch (e) {
        onFailure?.(mk(e, n));
        // CONTINUE mode keeps pushing so one inconsistency can compound into
        // the next, the way a long-lived prod pipeline does.
        if (!CONTINUE) return;
      }
    }
    void view.data;
  } catch (e) {
    onFailure?.(mk(e, script.length - 1));
  }
}

// Opt-in: this is a long-running search, not a regression gate.
const maybeTest = process.env.SWEEP === '1' ? test : test.skip;

maybeTest(
  'sweep: take over union-fan-in',
  () => {
    const byError = new Map<string, Failure>();
    const byShape = new Map<string, number>();
    const byErrorShape = new Map<string, number>();
    let runs = 0;
    let failures = 0;

    for (const shape of ACTIVE_SHAPES) {
      for (const seed of ACTIVE_SEEDS) {
        for (const dir of ACTIVE_DIRS) {
          for (const limit of ACTIVE_LIMITS) {
            for (const script of scripts(SCRIPT_LEN)) {
              const record = (f: Failure) => {
                failures++;
                if (!byError.has(f.error)) byError.set(f.error, f);
                byShape.set(f.shape, (byShape.get(f.shape) ?? 0) + 1);
                const k = `${f.error} @ ${f.shape}`;
                byErrorShape.set(k, (byErrorShape.get(k) ?? 0) + 1);
              };
              if (YIELD_SEEDS === 0) {
                runs++;
                runOne(shape, seed, dir, limit, script, record);
              } else {
                for (let s = 0; s < YIELD_SEEDS; s++) {
                  runs++;
                  runOne(
                    shape,
                    seed,
                    dir,
                    limit,
                    script,
                    record,
                    runs * 2654435761 + s,
                  );
                }
              }
            }
          }
        }
      }
    }

    // oxlint-disable-next-line no-console -- the sweep result IS the output
    console.log(
      JSON.stringify(
        {
          runs,
          failures,
          byShape: Object.fromEntries(byShape),
          byErrorShape: Object.fromEntries(byErrorShape),
          distinctErrors: Array.from(byError, ([msg, f]) => ({
            error: msg,
            firstRepro: f,
          })),
        },
        null,
        2,
      ),
    );

    expect(byError.size).toBe(0);
  },
  1_800_000,
);
