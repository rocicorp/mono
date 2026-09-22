/**
 * Regression test for the prod `Bound should be set` crash (take.ts:448).
 *
 *   source -> UnionFanOut -> [filter | flipped-exists] -> UnionFanIn -> Take
 *
 * Root cause: `UnionFanIn.#pushInternalChange` probed each sibling branch with
 * `first(input.fetch(...))` to decide whether another branch already held the
 * row. `fetch` interleaves 'yield' sentinels for cooperative multitasking, so
 * an empty branch that happened to yield was misread as a branch holding the
 * row, and the add was silently dropped. `Take` then never saw the row its own
 * fetch could see, leaving `bound` unset for the following edit. Fixed by
 * `first(skipYields(...))` in union-fan-in.ts.
 *
 * Only reproduced when cooperative multitasking is in play: prod's view-syncer
 * constructs TableSource with a `shouldYield` callback
 * (pipeline-driver.ts:1095), so sentinels thread through every fetch and push
 * stream. Without yields, 3M+ zqlite runs found nothing; with them, a
 * 2-operation script was enough.
 *
 * Run under `zql` (MemorySource) and `zqlite-zql-test` (SQLite TableSource).
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
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import type {Source} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {wrapSourcesWithRandomYield} from '../ivm/test/random-yield-source.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
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

/**
 * The yield schedule decides where sentinels land, so scan seeds rather than
 * pinning one: before the fix the MemorySource and SQLite sources broke on
 * different schedules, each within the first seed tried.
 */
function findFailingSeed(): {seed: number; error: string} | undefined {
  for (let i = 0; i < 400; i++) {
    const seed = 31685999679057 + i * 2654435761;
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
    sources = wrapSourcesWithRandomYield(sources, mulberry32(seed), 0.3);
    const chatSource = must(sources.chat);
    const msgSource = must(sources.message);

    const c1: Row = {id: 'c1', lastMessageAt: null, mode: 'b'};
    const c2: Row = {id: 'c2', lastMessageAt: null, mode: 'b'};
    consume(chatSource.push(makeSourceChangeAdd(c1)));
    consume(chatSource.push(makeSourceChangeAdd(c2)));

    const delegate = new QueryDelegateImpl({sources});
    try {
      const view = delegate.materialize(
        newQuery(schema, 'chat')
          .where(({or, cmp, exists}) =>
            or(
              cmp('mode', '=', 'a'),
              exists('messages', m => m.where('body', '=', 'x'), {flip: true}),
            ),
          )
          .orderBy('lastMessageAt', 'desc')
          .orderBy('id', 'desc')
          .limit(1),
      );
      // 1. c1 starts matching via the flipped-exists branch.
      consume(
        msgSource.push(
          makeSourceChangeAdd({id: 'mx1', chatId: 'c1', body: 'x'}),
        ),
      );
      // 2. The ordinary "a message arrived" update on the sort key.
      consume(
        chatSource.push(makeSourceChangeEdit({...c1, lastMessageAt: 75}, c1)),
      );
      void view.data;
    } catch (e) {
      return {seed, error: e instanceof Error ? e.message : String(e)};
    }
  }
  return undefined;
}

test('take over union-fan-in survives cooperative yields', () => {
  // Before the fix this returned on the first seed tried, on both source
  // implementations, with 'Bound should be set' -- the exact assert from the
  // prod view-syncer crash (take.ts:448).
  expect(findFailingSeed()).toBeUndefined();
});
