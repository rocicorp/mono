/** Which yield stream does the 2-op repro need: fetch, push, or both? */
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
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import type {Source} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {
  wrapSourcesWithModeYield,
  type YieldMode,
} from '../ivm/test/mode-yield-source.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const chat = table('chat')
  .columns({id: string(), lastMessageAt: number().optional(), mode: string()})
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

type Window = 'always' | 'hydration' | 'post';

function attempt(
  mode: YieldMode,
  seed: number,
  tables: 'all' | 'chat' | 'message' = 'all',
  window: Window = 'always',
  shape: 'flip' | 'noflip' | 'nosubquery' = 'flip',
): string | undefined {
  let sources: Record<string, Source> = {
    chat: createSource(
      lc,
      testLogConfig,
      'chat',
      schema.tables.chat.columns,
      schema.tables.chat.primaryKey,
    ),
    message: createSource(
      lc,
      testLogConfig,
      'message',
      schema.tables.message.columns,
      schema.tables.message.primaryKey,
    ),
  };
  const base = mulberry32(seed);
  // Returning 1 disables yielding (the wrapper tests `rng() < p`).
  let live = window !== 'post';
  const rng = () => (live ? base() : 1);
  const wrapped = wrapSourcesWithModeYield(sources, rng, mode, 0.3);
  sources =
    tables === 'all' ? wrapped : {...sources, [tables]: must(wrapped[tables])};
  const chatSource = must(sources.chat);
  const msgSource = must(sources.message);
  const c1: Row = {id: 'c1', lastMessageAt: null, mode: 'b'};
  const c2: Row = {id: 'c2', lastMessageAt: null, mode: 'b'};
  consume(chatSource.push(makeSourceChangeAdd(c1)));
  consume(chatSource.push(makeSourceChangeAdd(c2)));
  const delegate = new QueryDelegateImpl({sources});
  try {
    const q = newQuery(schema, 'chat')
      .orderBy('lastMessageAt', 'desc')
      .orderBy('id', 'desc')
      .limit(1);
    const view = delegate.materialize(
      shape === 'nosubquery'
        ? q.where(({cmp}) => cmp('mode', '!=', 'zzz'))
        : q.where(({or, cmp, exists}) =>
            or(
              cmp('mode', '=', 'a'),
              exists('messages', m => m.where('body', '=', 'x'), {
                flip: shape === 'flip',
              }),
            ),
          ),
    );
    if (window === 'hydration') live = false;
    if (window === 'post') live = true;
    consume(
      msgSource.push(makeSourceChangeAdd({id: 'mx1', chatId: 'c1', body: 'x'})),
    );
    consume(
      chatSource.push(makeSourceChangeEdit({...c1, lastMessageAt: 75}, c1)),
    );
    void view.data;
  } catch (e) {
    return e instanceof Error ? e.message : String(e);
  }
  return undefined;
}

// Opt-in diagnostic (14k pipeline runs), not a regression gate: run with
// SWEEP=1 under `zql` and/or `zqlite-zql-test`.
const maybeTest = process.env.SWEEP === '1' ? test : test.skip;

maybeTest(
  'yield-mode breakdown for the 2-op repro',
  () => {
    const report: Record<string, Record<string, number>> = {};
    const cases: [
      YieldMode,
      'all' | 'chat' | 'message',
      Window,
      ('flip' | 'noflip' | 'nosubquery')?,
    ][] = [
      ['both', 'all', 'always'],
      ['fetch', 'all', 'always'],
      ['push', 'all', 'always'],
      ['fetch', 'chat', 'always'],
      ['fetch', 'message', 'always'],
      ['fetch', 'chat', 'hydration'],
      ['fetch', 'chat', 'post'],
      // Controls: same script + same yield window, but no UnionFanOut/FanIn.
      ['fetch', 'chat', 'post', 'noflip'],
      ['fetch', 'chat', 'post', 'nosubquery'],
      ['fetch', 'all', 'always', 'noflip'],
    ];
    for (const [mode, tables, window, shape = 'flip'] of cases) {
      const counts: Record<string, number> = {};
      for (let i = 0; i < 2000; i++) {
        const err = attempt(
          mode,
          31685999679057 + i * 2654435761,
          tables,
          window,
          shape,
        );
        const k = err ?? '(no error)';
        counts[k] = (counts[k] ?? 0) + 1;
      }
      report[`${shape}: ${mode}/${tables}/${window}`] = counts;
    }
    // oxlint-disable-next-line no-console -- the breakdown IS the output
    console.log(JSON.stringify(report, null, 2));
    expect(Object.keys(report).length).toBeGreaterThan(0);
  },
  600_000,
);
