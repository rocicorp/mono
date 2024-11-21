import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import type {JSONValue} from '../../../shared/src/json.js';
import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {Catch} from './catch.js';
import {SetOfConstraint} from './constraint.js';
import type {Node} from './data.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {type PushMessage, Snitch, type SnitchMessage} from './snitch.js';
import {Take} from './take.js';

suite('take with no partition', () => {
  const base = {
    columns: {id: {type: 'string'}, created: {type: 'number'}},
    primaryKey: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
    partitionKey: undefined,
  } as const;

  takeTest({
    ...base,
    name: 'limit 0',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ],
    limit: 0,
    partitions: [
      {
        partitionValue: undefined,
        expectedMessages: [[], [], [['takeSnitch', 'cleanup', {}]]],
        expectedStorage: {},
        expectedHydrate: [],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'no data',
    sourceRows: [],
    limit: 5,
    partitions: [
      {
        partitionValue: undefined,
        expectedMessages: [
          [['takeSnitch', 'fetch', {}]],
          [],
          [['takeSnitch', 'cleanup', {}]],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: undefined,
            size: 0,
          },
        },
        expectedHydrate: [],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'less data than limit',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ],
    limit: 5,
    partitions: [
      {
        partitionValue: undefined,
        expectedMessages: [
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'cleanup', {}]],
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
        expectedHydrate: [
          {row: {id: 'i1', created: 100}, relationships: {}},
          {row: {id: 'i2', created: 200}, relationships: {}},
          {row: {id: 'i3', created: 300}, relationships: {}},
        ],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'data size and limit equal',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
      {id: 'i4', created: 400},
      {id: 'i5', created: 500},
    ],
    limit: 5,
    partitions: [
      {
        partitionValue: undefined,
        expectedMessages: [
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'cleanup', {}]],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 500,
              id: 'i5',
            },
            size: 5,
          },
          'maxBound': {
            created: 500,
            id: 'i5',
          },
        },
        expectedHydrate: [
          {row: {id: 'i1', created: 100}, relationships: {}},
          {row: {id: 'i2', created: 200}, relationships: {}},
          {row: {id: 'i3', created: 300}, relationships: {}},
          {row: {id: 'i4', created: 400}, relationships: {}},
          {row: {id: 'i5', created: 500}, relationships: {}},
        ],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'more data than limit',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
      {id: 'i4', created: 400},
      {id: 'i5', created: 500},
      {id: 'i6', created: 600},
    ],
    limit: 5,
    partitions: [
      {
        partitionValue: undefined,
        expectedMessages: [
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'cleanup', {}]],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 500,
              id: 'i5',
            },
            size: 5,
          },
          'maxBound': {
            created: 500,
            id: 'i5',
          },
        },
        expectedHydrate: [
          {row: {id: 'i1', created: 100}, relationships: {}},
          {row: {id: 'i2', created: 200}, relationships: {}},
          {row: {id: 'i3', created: 300}, relationships: {}},
          {row: {id: 'i4', created: 400}, relationships: {}},
          {row: {id: 'i5', created: 500}, relationships: {}},
        ],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'limit 1',
    sourceRows: [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ],
    limit: 1,
    partitions: [
      {
        partitionValue: undefined,
        expectedMessages: [
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'fetch', {}]],
          [['takeSnitch', 'cleanup', {}]],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 100,
              id: 'i1',
            },
            size: 1,
          },
          'maxBound': {
            created: 100,
            id: 'i1',
          },
        },
        expectedHydrate: [{row: {id: 'i1', created: 100}, relationships: {}}],
      },
    ],
  });
});

suite('take with partition', () => {
  const base = {
    columns: {
      id: {type: 'string'},
      issueID: {type: 'string'},
      created: {type: 'number'},
    },
    primaryKey: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
    partitionKey: 'issueID',
  } as const;

  takeTest({
    ...base,
    name: 'limit 0',
    sourceRows: [
      {id: 'c1', issueID: 'i1', created: 100},
      {id: 'c2', issueID: 'i1', created: 200},
      {id: 'c3', issueID: 'i1', created: 300},
    ],
    limit: 0,
    partitions: [
      {
        partitionValue: 'i1',
        expectedMessages: [
          [],
          [],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
        ],
        expectedStorage: {},
        expectedHydrate: [],
      },
      {
        partitionValue: 'i2',
        expectedMessages: [
          [],
          [],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
        ],
        expectedStorage: {},
        expectedHydrate: [],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'no data',
    sourceRows: [],
    limit: 5,
    partitions: [
      {
        partitionValue: 'i1',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: undefined,
            size: 0,
          },
        },
        expectedHydrate: [],
      },
      {
        partitionValue: 'i2',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i2"]': {
            bound: undefined,
            size: 0,
          },
        },
        expectedHydrate: [],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'less data than limit',
    sourceRows: [
      {id: 'c1', issueID: 'i1', created: 100},
      {id: 'c2', issueID: 'i1', created: 200},
      {id: 'c3', issueID: 'i1', created: 300},
      {id: 'c4', issueID: 'i2', created: 400},
      {id: 'c5', issueID: 'i2', created: 500},
    ],
    limit: 5,
    partitions: [
      {
        partitionValue: 'i0',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i0',
                },
              },
            ],
          ],
          [],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i0',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i0"]': {
            bound: undefined,
            size: 0,
          },
        },
        expectedHydrate: [],
      },
      {
        partitionValue: 'i1',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {id: 'c3', issueID: 'i1', created: 300},
            size: 3,
          },
          'maxBound': {id: 'c3', issueID: 'i1', created: 300},
        },
        expectedHydrate: [
          {row: {id: 'c1', issueID: 'i1', created: 100}, relationships: {}},
          {row: {id: 'c2', issueID: 'i1', created: 200}, relationships: {}},
          {row: {id: 'c3', issueID: 'i1', created: 300}, relationships: {}},
        ],
      },
      {
        partitionValue: 'i2',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i2"]': {
            bound: {id: 'c5', issueID: 'i2', created: 500},
            size: 2,
          },
          'maxBound': {id: 'c5', issueID: 'i2', created: 500},
        },
        expectedHydrate: [
          {row: {id: 'c4', issueID: 'i2', created: 400}, relationships: {}},
          {row: {id: 'c5', issueID: 'i2', created: 500}, relationships: {}},
        ],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'data size and limit equal',
    sourceRows: [
      {id: 'c1', issueID: 'i1', created: 100},
      {id: 'c2', issueID: 'i1', created: 200},
      {id: 'c3', issueID: 'i1', created: 300},
      {id: 'c4', issueID: 'i2', created: 400},
      {id: 'c5', issueID: 'i2', created: 500},
      {id: 'c6', issueID: 'i2', created: 600},
    ],
    limit: 3,
    partitions: [
      {
        partitionValue: 'i1',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {id: 'c3', issueID: 'i1', created: 300},
            size: 3,
          },
          'maxBound': {id: 'c3', issueID: 'i1', created: 300},
        },
        expectedHydrate: [
          {row: {id: 'c1', issueID: 'i1', created: 100}, relationships: {}},
          {row: {id: 'c2', issueID: 'i1', created: 200}, relationships: {}},
          {row: {id: 'c3', issueID: 'i1', created: 300}, relationships: {}},
        ],
      },
      {
        partitionValue: 'i2',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i2"]': {
            bound: {id: 'c6', issueID: 'i2', created: 600},
            size: 3,
          },
          'maxBound': {id: 'c6', issueID: 'i2', created: 600},
        },
        expectedHydrate: [
          {row: {id: 'c4', issueID: 'i2', created: 400}, relationships: {}},
          {row: {id: 'c5', issueID: 'i2', created: 500}, relationships: {}},
          {row: {id: 'c6', issueID: 'i2', created: 600}, relationships: {}},
        ],
      },
    ],
  });

  takeTest({
    ...base,
    name: 'more data than limit',
    sourceRows: [
      {id: 'c1', issueID: 'i1', created: 100},
      {id: 'c2', issueID: 'i1', created: 200},
      {id: 'c3', issueID: 'i1', created: 300},
      {id: 'c4', issueID: 'i2', created: 400},
      {id: 'c5', issueID: 'i2', created: 500},
      {id: 'c6', issueID: 'i2', created: 600},
      {id: 'c7', issueID: 'i1', created: 700},
      {id: 'c8', issueID: 'i2', created: 800},
    ],
    limit: 3,
    partitions: [
      {
        partitionValue: 'i1',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i1',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {id: 'c3', issueID: 'i1', created: 300},
            size: 3,
          },
          'maxBound': {id: 'c3', issueID: 'i1', created: 300},
        },
        expectedHydrate: [
          {row: {id: 'c1', issueID: 'i1', created: 100}, relationships: {}},
          {row: {id: 'c2', issueID: 'i1', created: 200}, relationships: {}},
          {row: {id: 'c3', issueID: 'i1', created: 300}, relationships: {}},
        ],
      },
      {
        partitionValue: 'i2',
        expectedMessages: [
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'fetch',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
          [
            [
              'takeSnitch',
              'cleanup',
              {
                constraint: {
                  issueID: 'i2',
                },
              },
            ],
          ],
        ],
        expectedStorage: {
          '["take","i2"]': {
            bound: {id: 'c6', issueID: 'i2', created: 600},
            size: 3,
          },
          'maxBound': {id: 'c6', issueID: 'i2', created: 600},
        },
        expectedHydrate: [
          {row: {id: 'c4', issueID: 'i2', created: 400}, relationships: {}},
          {row: {id: 'c5', issueID: 'i2', created: 500}, relationships: {}},
          {row: {id: 'c6', issueID: 'i2', created: 600}, relationships: {}},
        ],
      },
    ],
  });
});

function takeTest(t: TakeTest) {
  test(t.name, () => {
    const log: SnitchMessage[] = [];
    const source = new MemorySource('table', t.columns, t.primaryKey);
    for (const row of t.sourceRows) {
      source.push({type: 'add', row});
    }
    const snitch = new Snitch(
      source.connect(t.sort || [['id', 'asc']]),
      'takeSnitch',
      log,
    );
    const memoryStorage = new MemoryStorage();

    const {partitionKey} = t;
    const take = new Take(snitch, memoryStorage, t.limit, partitionKey);
    if (t.partitionKey === undefined) {
      assert(t.partitions.length === 1);
      assert(t.partitions[0].partitionValue === undefined);
    }
    for (const partition of t.partitions) {
      const {partitionValue} = partition;
      const fetches = ['fetch', 'fetch', 'cleanup'] as const;
      for (let i = 0; i < fetches.length; i++) {
        const fetchType = fetches[i];
        log.length = 0;

        const c = new Catch(take);
        const r = c[fetchType](
          partitionKey && partitionValue
            ? {
                constraint: {
                  [partitionKey]: partitionValue,
                },
              }
            : undefined,
        );

        expect(r).toEqual(partition.expectedHydrate);
        expect(c.pushes).toEqual([]);
        if (fetchType === 'fetch') {
          expect(memoryStorage.cloneData()).toEqual(partition.expectedStorage);
        } else {
          fetchType satisfies 'cleanup';
          expect(memoryStorage.cloneData()).toEqual(
            'maxBound' in partition.expectedStorage
              ? {maxBound: partition.expectedStorage.maxBound}
              : {},
          );
        }

        let expectedMessages = partition.expectedMessages[i] as Exclude<
          SnitchMessage,
          PushMessage
        >[];
        if (fetchType === 'fetch') {
          expectedMessages = expectedMessages.map(([name, _, arg]) => [
            name,
            'fetch',
            arg,
          ]);
        } else if (fetchType === 'cleanup') {
          // For cleanup, the last fetch for any constraint should be a cleanup.
          // Others should be fetch.
          const seen = new SetOfConstraint();
          for (let i = expectedMessages.length - 1; i >= 0; i--) {
            const [name, _, req] = expectedMessages[i];
            if (!(req.constraint && seen.has(req.constraint))) {
              expectedMessages[i] = [name, 'cleanup', req];
            } else {
              expectedMessages[i] = [name, 'fetch', req];
            }
            req.constraint && seen.add(req.constraint);
          }
        }
        expect(log).toEqual(expectedMessages);
      }
    }
  });
}

type TakeTest = {
  name: string;
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sourceRows: Row[];
  sort?: Ordering | undefined;
  limit: number;
  partitionKey: string | undefined;
  partitions: {
    partitionValue: string | undefined;
    expectedMessages: [SnitchMessage[], SnitchMessage[], SnitchMessage[]];
    expectedStorage: Record<string, JSONValue>;
    expectedHydrate: Node[];
  }[];
};
