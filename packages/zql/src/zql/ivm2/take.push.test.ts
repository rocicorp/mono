import {test, expect, suite} from 'vitest';
import {Ordering} from '../ast2/ast.js';
import {Row, Value} from './data.js';
import {ValueType} from './schema.js';
import {Snitch, SnitchMessage} from './snitch.js';
import {Take} from './take.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {Catch} from './catch.js';
import {JSONValue} from 'shared/src/json.js';
import {SourceChange} from './source.js';
import {Change} from './change.js';

suite('take with no partition', () => {
  const base = {
    columns: {id: 'string' as const, created: 'number' as const},
    primaryKeys: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ] as const,
    partition: undefined,
  };
  takeTest({
    ...base,
    name: 'less than limit add row at start',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ],
    limit: 5,
    pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
    expectedMessages: [
      ['takeSnitch', 'push', {type: 'add', row: {id: 'i4', created: 50}}],
    ],
    expectedStorage: {
      '["take",null]': {
        bound: {
          created: 300,
          id: 'i3',
        },
        size: 4,
      },
      'maxBound': {
        created: 300,
        id: 'i3',
      },
    },
    expectedOutput: [
      {type: 'add', node: {row: {id: 'i4', created: 50}, relationships: {}}},
    ],
  });

  takeTest({
    ...base,
    name: 'less than limit add row at end',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ],
    limit: 5,
    pushes: [{type: 'add', row: {id: 'i4', created: 350}}],
    expectedMessages: [
      ['takeSnitch', 'push', {type: 'add', row: {id: 'i4', created: 350}}],
    ],
    expectedStorage: {
      '["take",null]': {
        bound: {
          created: 350,
          id: 'i4',
        },
        size: 4,
      },
      'maxBound': {
        created: 350,
        id: 'i4',
      },
    },
    expectedOutput: [
      {type: 'add', node: {row: {id: 'i4', created: 350}, relationships: {}}},
    ],
  });
});

function takeTest(t: TakeTest) {
  test(t.name, () => {
    const log: SnitchMessage[] = [];
    const source = new MemorySource('table', t.columns, t.primaryKeys);
    for (const row of t.sourceRows) {
      source.push({type: 'add', row});
    }
    const snitch = new Snitch(
      source.connect(t.sort || [['id', 'asc']]),
      'takeSnitch',
      log,
    );
    const memoryStorage = new MemoryStorage();
    const partitionKey = t.partition?.key;

    const take = new Take(snitch, memoryStorage, t.limit, partitionKey);
    const c = new Catch(take);
    for (const partitionValue of t.partition?.values || [undefined]) {
      c.fetch(
        partitionKey && partitionValue
          ? {
              constraint: {
                key: partitionKey,
                value: partitionValue,
              },
            }
          : undefined,
      );
      expect(c.pushes).toEqual([]);
    }
    log.length = 0;
    for (const change of t.pushes) {
      source.push(change);
    }
    expect(log).toEqual(t.expectedMessages);
    expect(memoryStorage.cloneData()).toEqual(t.expectedStorage);
    expect(c.pushes).toEqual(t.expectedOutput);
  });
}

type TakeTest = {
  name: string;
  columns: Record<string, ValueType>;
  primaryKeys: readonly string[];
  sourceRows: Row[];
  sort?: Ordering | undefined;
  limit: number;
  partition:
    | {
        key: string;
        values: Value[];
      }
    | undefined;
  pushes: SourceChange[];
  expectedMessages: SnitchMessage[];
  expectedStorage: Record<string, JSONValue>;
  expectedOutput: Change[];
};
