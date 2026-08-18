/**
 * Ground-truth check for the take-over-union-fan-in sequence.
 *
 * Runs the same 5 mutations as take-union-bound.repro.test.ts and asserts the
 * view against hand-computed expected contents at every step. Run under both
 * `zql` (MemorySource) and `zqlite-zql-test` (SQLite TableSource) to see which
 * source produces the wrong answer.
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

test('take over union-fan-in matches ground truth at every step', () => {
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

  const c1 = {id: 'c1', lastMessageAt: 10, mode: 'b'};
  let c2: Row = {id: 'c2', lastMessageAt: 20, mode: 'b'};
  consume(chatSource.push(makeSourceChangeAdd(c1)));
  consume(chatSource.push(makeSourceChangeAdd(c2)));

  const delegate = new QueryDelegateImpl({sources});
  const view = delegate.materialize(
    newQuery(schema, 'chat')
      .where(({or, cmp, exists}) =>
        or(
          cmp('lastMessageAt', '>', 100),
          exists('messages', m => m.where('body', '=', 'x'), {flip: true}),
        ),
      )
      .orderBy('lastMessageAt', 'asc')
      .orderBy('id', 'asc')
      .limit(1),
  );

  const ids = () => (view.data as {id: string}[]).map(r => r.id);
  const step = (label: string, fn: () => void, expected: string[]) => {
    try {
      fn();
    } catch (e) {
      throw new Error(
        `${label}: threw ${e instanceof Error ? e.message : String(e)}`,
      );
    }
    expect(ids(), label).toEqual(expected);
  };

  //                                                    matching   window(limit 1, asc)
  expect(ids(), 'hydrate').toEqual([]); //              none       []
  const mx1 = {id: 'mx1', chatId: 'c1', body: 'x'};
  const mx2 = {id: 'mx2', chatId: 'c2', body: 'x'};

  step('1 +mx1', () => consume(msgSource.push(makeSourceChangeAdd(mx1))), [
    'c1', //                                            c1(10)     [c1]
  ]);
  step('2 +mx2', () => consume(msgSource.push(makeSourceChangeAdd(mx2))), [
    'c1', //                                            c1(10),c2(20) [c1]
  ]);
  step(
    '3 c2.lma->null',
    () => {
      const next = {...c2, lastMessageAt: null};
      consume(chatSource.push(makeSourceChangeEdit(next, c2)));
      c2 = next;
    },
    ['c2'], //                                          c2(null),c1(10) [c2]
  );
  step('4 -mx1', () => consume(msgSource.push(makeSourceChangeRemove(mx1))), [
    'c2', //                                            c2(null)   [c2]
  ]);
  step(
    '5 c2.lma->200',
    () => {
      const next = {...c2, lastMessageAt: 200};
      consume(chatSource.push(makeSourceChangeEdit(next, c2)));
      c2 = next;
    },
    ['c2'], //                                          c2(200)    [c2]
  );
});
