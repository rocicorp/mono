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
    columns: {
      id: 'string' as const,
      created: 'number' as const,
    },
    primaryKeys: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ] as const,
    partition: undefined,
  };

  suite('add', () => {
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

    takeTest({
      ...base,
      name: 'at limit add row after bound',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'i5', created: 350}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i5', created: 350}}],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [],
    });

    takeTest({
      ...base,
      name: 'at limit add row at start',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'i5', created: 50}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i5', created: 50}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 200,
            id: 'i2',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
        {type: 'add', node: {row: {id: 'i5', created: 50}, relationships: {}}},
      ],
    });

    takeTest({
      ...base,
      name: 'at limit add row at end',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'i5', created: 250}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i5', created: 250}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 250,
            id: 'i5',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
        {type: 'add', node: {row: {id: 'i5', created: 250}, relationships: {}}},
      ],
    });
  });

  suite('remove', () => {
    takeTest({
      ...base,
      name: 'less than limit remove row at start',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
        [
          'takeSnitch',
          'fetch',
          {
            start: {
              basis: 'before',
              row: {
                created: 300,
                id: 'i3',
              },
            },
          },
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i1', created: 100}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'less than limit remove row at end',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i3', created: 300}}],
        [
          'takeSnitch',
          'fetch',
          {
            start: {
              basis: 'before',
              row: {
                created: 300,
                id: 'i3',
              },
            },
          },
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 200,
            id: 'i2',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row after bound',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i4', created: 400}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i4', created: 400}}],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at start with row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 400,
            id: 'i4',
          },
          size: 3,
        },
        'maxBound': {
          created: 400,
          id: 'i4',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i1', created: 100}, relationships: {}},
        },
        {
          type: 'add',
          node: {row: {id: 'i4', created: 400}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at start no row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i1', created: 100}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at end with row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i3', created: 300}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 400,
            id: 'i4',
          },
          size: 3,
        },
        'maxBound': {
          created: 400,
          id: 'i4',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
        {
          type: 'add',
          node: {row: {id: 'i4', created: 400}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at end, no row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i3', created: 300}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 200,
            id: 'i2',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
      ],
    });
  });
});

suite('take with partition', () => {
  const base = {
    columns: {
      id: 'string' as const,
      issueID: 'string' as const,
      created: 'number' as const,
    },
    primaryKeys: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ] as const,
  };

  suite('add', () => {
    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'less than limit add row at start',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
      ],
      limit: 5,
      pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}},
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 3,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [
        {
          type: 'add',
          node: {
            row: {id: 'c6', issueID: 'i2', created: 150},
            relationships: {},
          },
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit add row at end',
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        // 580 to test that it constrains looking for previous
        // to constraint issueID: 'i2'
        {id: 'c3', issueID: 'i1', created: 580},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
        {id: 'c6', issueID: 'i2', created: 600},
        {id: 'c7', issueID: 'i2', created: 700},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'c8', issueID: 'i2', created: 550}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c8', issueID: 'i2', created: 550}},
        ],
        [
          'takeSnitch',
          'fetch',
          {
            constraint: {
              key: 'issueID',
              value: 'i2',
            },
            start: {
              basis: 'before',
              row: {id: 'c6', issueID: 'i2', created: 600},
            },
          },
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 580},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c8', issueID: 'i2', created: 550},
          size: 3,
        },
        'maxBound': {id: 'c6', issueID: 'i2', created: 600},
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {
            row: {id: 'c6', issueID: 'i2', created: 600},
            relationships: {},
          },
        },
        {
          type: 'add',
          node: {
            row: {id: 'c8', issueID: 'i2', created: 550},
            relationships: {},
          },
        },
      ],
    });

    takeTest({
      ...base,
      name: 'add with non-fetched partition value',
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'c6', issueID: '3', created: 550}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c6', issueID: '3', created: 550}},
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 2,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [],
    });
  });

  suite('remove', () => {
    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'less than limit remove row at start',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
        [
          'takeSnitch',
          'fetch',
          {
            constraint: {
              key: 'issueID',
              value: 'i1',
            },
            start: {
              basis: 'before',
              row: {id: 'c3', issueID: 'i1', created: 300},
            },
          },
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 2,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 2,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {
            row: {id: 'c1', issueID: 'i1', created: 100},
            relationships: {},
          },
        },
      ],
    });

    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'remove row unfetched partition',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
        {id: 'c6', issueID: 'i3', created: 600},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'c6', issueID: 'i3', created: 600}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'remove', row: {id: 'c6', issueID: 'i3', created: 600}},
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 2,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [],
    });
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
