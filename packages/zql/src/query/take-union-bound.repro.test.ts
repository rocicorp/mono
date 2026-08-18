/**
 * Minimal repro for the prod `Bound should be set` crash (take.ts:448),
 * reached through a UnionFanOut/UnionFanIn -- an OR containing a flipped
 * EXISTS -- sitting directly beneath a Take.
 *
 * Found by sweep in take-union-empty-window.sweep.test.ts. Uses MemorySource:
 * no SQLite, no NULL start-bound lowering, no PG/Zero schema drift. The
 * inconsistency is produced entirely inside the IVM graph.
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
import {consume} from '../ivm/stream.ts';
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

test('Bound should be set: take over union-fan-in with flipped exists', () => {
  const chatSchema = schema.tables.chat;
  const messageSchema = schema.tables.message;
  const sources = {
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
  const chatSource = must(sources.chat);
  const msgSource = must(sources.message);

  // Two chats, neither matching either OR branch yet.
  const c1 = {id: 'c1', lastMessageAt: 10, mode: 'b'};
  let c2: Row = {id: 'c2', lastMessageAt: 20, mode: 'b'};
  consume(chatSource.push(makeSourceChangeAdd(c1)));
  consume(chatSource.push(makeSourceChangeAdd(c2)));

  const delegate = new QueryDelegateImpl({sources});
  const q = newQuery(schema, 'chat')
    .where(({or, cmp, exists}) =>
      or(
        cmp('lastMessageAt', '>', 100),
        exists('messages', m => m.where('body', '=', 'x'), {flip: true}),
      ),
    )
    .orderBy('lastMessageAt', 'asc')
    .orderBy('id', 'asc')
    .limit(1);

  const view = delegate.materialize(q);
  expect(view.data).toEqual([]); // take window hydrates empty

  const mx1 = {id: 'mx1', chatId: 'c1', body: 'x'};
  const mx2 = {id: 'mx2', chatId: 'c2', body: 'x'};

  // 1. c1 starts matching via the flipped-exists branch.
  consume(msgSource.push(makeSourceChangeAdd(mx1)));
  // 2. c2 also starts matching via the flipped-exists branch.
  consume(msgSource.push(makeSourceChangeAdd(mx2)));
  // 3. c2's sort key goes NULL (a chat with no messages yet).
  //    This is ALREADY a violation: the take's refill fetch through the
  //    fan-in comes back empty for a row the union still holds. In prod
  //    this is the crash that gets logged and rethrown.
  const c2b = {...c2, lastMessageAt: null};
  expect(() => consume(chatSource.push(makeSourceChangeEdit(c2b, c2)))).toThrow(
    'Take: newBoundNode must be found during fetch',
  );
  c2 = c2b;

  // 4. c1 stops matching the exists branch. This drains the take window
  //    to size 0, leaving takeState.bound === undefined.
  consume(msgSource.push(makeSourceChangeRemove(mx1)));

  // 5. c2's sort key is set -- the ordinary "a message arrived" update.
  //    c2 still matches the exists branch (mx2 is still there), so the
  //    fan-in emits an EDIT into a Take whose bound is undefined.
  //    Frame-for-frame the prod stack:
  //      Take.#pushEditChange <- Take.push <- pushAccumulatedChanges
  //      <- UnionFanIn.fanOutDonePushing <- UnionFanOut.push
  const c2c = {...c2, lastMessageAt: 200};
  expect(() => consume(chatSource.push(makeSourceChangeEdit(c2c, c2)))).toThrow(
    'Bound should be set',
  );
});
