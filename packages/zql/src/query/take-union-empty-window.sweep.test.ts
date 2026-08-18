/**
 * Sweep hunting for `Bound should be set` (take.ts) reachable through a
 * UnionFanOut/UnionFanIn (an OR containing a flipped EXISTS) sitting directly
 * beneath a Take -- the pipeline shape in the prod stack trace:
 *
 *   source -> UnionFanOut -> [filter branch | flipped-exists branch] ->
 *   UnionFanIn -> Take
 *
 * Modelled on `chat.listRich`: nullable leading sort key (`lastMessageAt`),
 * secondary `id` sort, a top-level limit, and an OR whose branches mix a plain
 * predicate with a `whereExists`.
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
  .columns({
    id: string(),
    chatId: string(),
    body: string(),
  })
  .primaryKey('id');

const chatRelationships = relationships(chat, ({many}) => ({
  messages: many({
    sourceField: ['id'],
    destField: ['chatId'],
    destSchema: message,
  }),
}));

const schema = createSchema({
  tables: [chat, message],
  relationships: [chatRelationships],
});

const chatSchema = schema.tables.chat;
const messageSchema = schema.tables.message;

function makeSources() {
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
  } as Record<string, Source>;
}

// --------------------------------------------------------------------------
// query shapes
// --------------------------------------------------------------------------

type Dir = 'asc' | 'desc';
type Shape = {
  name: string;
  build: (limit: number, dir: Dir) => Query<'chat', typeof schema>;
};

const SHAPES: Shape[] = [
  {
    name: 'or(cmp, exists-flip)',
    build: (limit, dir) =>
      newQuery(schema, 'chat')
        .where(({or, cmp, exists}) =>
          or(
            cmp('mode', '=', 'a'),
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          ),
        )
        .orderBy('lastMessageAt', dir)
        .orderBy('id', dir)
        .limit(limit),
  },
  {
    name: 'or(exists-flip, exists-flip)',
    build: (limit, dir) =>
      newQuery(schema, 'chat')
        .where(({or, exists}) =>
          or(
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
            exists('messages', q => q.where('body', '=', 'y'), {flip: true}),
          ),
        )
        .orderBy('lastMessageAt', dir)
        .orderBy('id', dir)
        .limit(limit),
  },
  {
    name: 'and(cmp, or(cmp, exists-flip))',
    build: (limit, dir) =>
      newQuery(schema, 'chat')
        .where(({and, or, cmp, exists}) =>
          and(
            cmp('mode', '!=', 'zzz'),
            or(
              cmp('mode', '=', 'a'),
              exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
            ),
          ),
        )
        .orderBy('lastMessageAt', dir)
        .orderBy('id', dir)
        .limit(limit),
  },
  {
    name: 'or(cmp-on-sortkey, exists-flip)',
    build: (limit, dir) =>
      newQuery(schema, 'chat')
        .where(({or, cmp, exists}) =>
          or(
            cmp('lastMessageAt', '>', 100),
            exists('messages', q => q.where('body', '=', 'x'), {flip: true}),
          ),
        )
        .orderBy('lastMessageAt', dir)
        .orderBy('id', dir)
        .limit(limit),
  },
  {
    name: 'or(cmp, exists-noflip) [control]',
    build: (limit, dir) =>
      newQuery(schema, 'chat')
        .where(({or, cmp, exists}) =>
          or(
            cmp('mode', '=', 'a'),
            exists('messages', q => q.where('body', '=', 'x'), {flip: false}),
          ),
        )
        .orderBy('lastMessageAt', dir)
        .orderBy('id', dir)
        .limit(limit),
  },
];

// --------------------------------------------------------------------------
// data + mutation pool
// --------------------------------------------------------------------------

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
];

type Op = {name: string; run: (r: Runner) => void};

const OPS: Op[] = [
  // last_message_at transitions -- the hot path in a chat app
  {name: 'c1.lma null->200', run: r => r.editChat('c1', {lastMessageAt: 200})},
  {name: 'c1.lma ->null', run: r => r.editChat('c1', {lastMessageAt: null})},
  {name: 'c1.lma ->75', run: r => r.editChat('c1', {lastMessageAt: 75})},
  {name: 'c2.lma ->200', run: r => r.editChat('c2', {lastMessageAt: 200})},
  {name: 'c2.lma ->null', run: r => r.editChat('c2', {lastMessageAt: null})},
  // mode transitions -- flip which OR branch matches
  {name: 'c1.mode ->a', run: r => r.editChat('c1', {mode: 'a'})},
  {name: 'c1.mode ->b', run: r => r.editChat('c1', {mode: 'b'})},
  {name: 'c2.mode ->a', run: r => r.editChat('c2', {mode: 'a'})},
  {name: 'c2.mode ->b', run: r => r.editChat('c2', {mode: 'b'})},
  // exists-branch transitions
  {name: '+msg c1 x', run: r => r.addMessage('mx1', 'c1', 'x')},
  {name: '-msg c1 x', run: r => r.removeMessage('mx1')},
  {name: '+msg c2 x', run: r => r.addMessage('mx2', 'c2', 'x')},
  {name: '-msg c2 x', run: r => r.removeMessage('mx2')},
  {name: 'msg mx1 x->y', run: r => r.editMessage('mx1', {body: 'y'})},
  // chat add/remove
  {
    name: '+chat c9 (null,a)',
    run: r => r.addChat({id: 'c9', lastMessageAt: null, mode: 'a'}),
  },
  {name: '-chat c1', run: r => r.removeChat('c1')},
  {name: '-chat c2', run: r => r.removeChat('c2')},
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

// --------------------------------------------------------------------------
// sweep
// --------------------------------------------------------------------------

type Failure = {
  shape: string;
  seed: string;
  dir: Dir;
  limit: number;
  script: string[];
  error: string;
  stack?: string | undefined;
};

const SCRIPT_LEN = Number(process.env.SWEEP_LEN ?? 3);

function pick<T>(all: T[], env: string | undefined): T[] {
  if (!env) return all;
  return env.split(',').map(i => all[Number(i)]);
}

const ACTIVE_OPS = pick(OPS, process.env.SWEEP_OPS);
const ACTIVE_SHAPES = pick(SHAPES, process.env.SWEEP_SHAPES);
const ACTIVE_SEEDS = pick(SEEDS, process.env.SWEEP_SEEDS);
const ACTIVE_DIRS = pick(['desc', 'asc'] as Dir[], process.env.SWEEP_DIRS);
const ACTIVE_LIMITS = pick([1, 2], process.env.SWEEP_LIMITS);
const ONLY_LEN = process.env.SWEEP_ONLY_LEN === '1';
const CONTINUE = process.env.SWEEP_CONTINUE === '1';

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
): Failure | undefined {
  const sources = makeSources();
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
    stack: e instanceof Error ? e.stack : undefined,
  });

  let first: Failure | undefined;
  try {
    const view = delegate.materialize(shape.build(limit, dir));
    for (let n = 0; n < script.length; n++) {
      try {
        ACTIVE_OPS[script[n]].run(runner);
      } catch (e) {
        const f = mk(e, n);
        if (!first) first = f;
        onFailure?.(f);
        // CONTINUE mode: keep pushing so one inconsistency can compound
        // into the next, the way a long-lived prod pipeline does.
        if (!CONTINUE) return first;
      }
    }
    void view.data;
  } catch (e) {
    const f = mk(e, script.length - 1);
    if (!first) first = f;
    onFailure?.(f);
  }
  return first;
}

// Opt-in: this is a long-running search, not a regression gate.
// Run with SWEEP=1 (plus the SWEEP_* knobs documented above).
const maybeTest = process.env.SWEEP === '1' ? test : test.skip;

maybeTest(
  'sweep: take over union-fan-in, empty window + edit',
  () => {
    const failures: Failure[] = [];
    const byError = new Map<string, Failure>();
    const byShape = new Map<string, number>();
    const byErrorShape = new Map<string, number>();
    let runs = 0;

    for (const shape of ACTIVE_SHAPES) {
      for (const seed of ACTIVE_SEEDS) {
        for (const dir of ACTIVE_DIRS) {
          for (const limit of ACTIVE_LIMITS) {
            for (const script of scripts(SCRIPT_LEN)) {
              runs++;
              const record = (f: Failure) => {
                failures.push(f);
                if (!byError.has(f.error)) byError.set(f.error, f);
                byShape.set(f.shape, (byShape.get(f.shape) ?? 0) + 1);
                const k = `${f.error} @ ${f.shape}`;
                byErrorShape.set(k, (byErrorShape.get(k) ?? 0) + 1);
              };
              runOne(shape, seed, dir, limit, script, record);
            }
          }
        }
      }
    }

    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          runs,
          failures: failures.length,
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
  900_000,
);
