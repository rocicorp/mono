/**
 * Proves the take-over-union-fan-in pipeline is actually built, and built the
 * same way, for both MemorySource (zql) and the SQLite TableSource
 * (zqlite-zql-test). If the union were absent under one source, a sweep that
 * finds nothing there would prove nothing.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Input} from '../ivm/operator.ts';
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

class ShapeRecordingDelegate extends QueryDelegateImpl {
  readonly names: string[] = [];
  override decorateInput(input: Input, name: string): Input {
    this.names.push(name);
    return super.decorateInput(input, name);
  }
}

function shapeFor(flip: boolean | undefined): string[] {
  const chatSchema = schema.tables.chat;
  const messageSchema = schema.tables.message;
  const delegate = new ShapeRecordingDelegate({
    sources: {
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
    },
  });
  delegate.materialize(
    newQuery(schema, 'chat')
      .where(({or, cmp, exists}) =>
        or(
          cmp('lastMessageAt', '>', 100),
          exists(
            'messages',
            m => m.where('body', '=', 'x'),
            flip === undefined ? undefined : {flip},
          ),
        ),
      )
      .orderBy('lastMessageAt', 'asc')
      .orderBy('id', 'asc')
      .limit(1),
  );
  return delegate.names.map(n => n.slice(n.lastIndexOf(':') + 1));
}

test('an EXISTS lowered without an explicit flip builds no union', () => {
  // The fuzzer's `lower()` calls `.whereExists(rel, sub)` with no flip option and
  // its driver never runs the planner, so every fuzzed EXISTS takes this path.
  expect(shapeFor(undefined)).not.toContain('ufo');
  expect(shapeFor(undefined)).not.toContain('ufi');
  expect(shapeFor(false)).not.toContain('ufi');
  // Only an explicit flip builds the fan-out/fan-in pair.
  expect(shapeFor(true)).toContain('ufi');
});

test('take sits above a union fan-in for both source implementations', () => {
  const chatSchema = schema.tables.chat;
  const messageSchema = schema.tables.message;
  const delegate = new ShapeRecordingDelegate({
    sources: {
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
    },
  });

  delegate.materialize(
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

  const suffixes = delegate.names.map(n => n.slice(n.lastIndexOf(':') + 1));
  // The union fan-out/fan-in pair and the take must all be present, and the
  // take must be built after (i.e. above) the fan-in.
  expect(suffixes).toContain('ufo');
  expect(suffixes).toContain('ufi');
  expect(delegate.names.some(n => n.endsWith(':take'))).toBe(true);
  expect(suffixes.indexOf('ufi')).toBeLessThan(
    suffixes.findIndex(n => n === 'take'),
  );
});
