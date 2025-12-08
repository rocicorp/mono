import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from './catch.ts';
import type {Node} from './data.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {FetchRequest} from './operator.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import type {Stream} from './stream.ts';
import {Take, type PartitionKey} from './take.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {consume} from './stream.ts';

const lc = createSilentLogContext();

suite('take with no partition', () => {
  const base = {
    columns: {id: {type: 'string'}, created: {type: 'number'}},
    primaryKey: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
    partitionKey: undefined,
    partitionValues: [undefined],
  } as const;

  test('limit 0', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 0,
    });
    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`{}`);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('no data', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [],
      limit: 5,
    });
    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take"]": {
          "bound": undefined,
          "size": 0,
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('less data than limit', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
    });
    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take"]": {
          "bound": {
            "created": 300,
            "id": "i3",
          },
          "size": 3,
        },
        "maxBound": {
          "created": 300,
          "id": "i3",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 300,
            "id": "i3",
          },
        },
      ]
    `);
  });

  test('data size and limit equal', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
        {id: 'i5', created: 500},
      ],
      limit: 5,
    });
    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take"]": {
          "bound": {
            "created": 500,
            "id": "i5",
          },
          "size": 5,
        },
        "maxBound": {
          "created": 500,
          "id": "i5",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 300,
            "id": "i3",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 400,
            "id": "i4",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 500,
            "id": "i5",
          },
        },
      ]
    `);
  });

  test('more data than limit', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
        {id: 'i5', created: 500},
        {id: 'i6', created: 600},
      ],
      limit: 5,
    });
    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take"]": {
          "bound": {
            "created": 500,
            "id": "i5",
          },
          "size": 5,
        },
        "maxBound": {
          "created": 500,
          "id": "i5",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 300,
            "id": "i3",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 400,
            "id": "i4",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 500,
            "id": "i5",
          },
        },
      ]
    `);
  });

  test('limit 1', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 1,
    });
    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take"]": {
          "bound": {
            "created": 100,
            "id": "i1",
          },
          "size": 1,
        },
        "maxBound": {
          "created": 100,
          "id": "i1",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "i1",
          },
        },
      ]
    `);
  });
});

class ThrowingSnitch extends Snitch {
  fetch(_: FetchRequest): Stream<Node> {
    throw new Error('ThrowingSnitch error');
  }
}

test('exception during hydrate', () => {
  const columns = {id: {type: 'string'}, created: {type: 'number'}} as const;
  const primaryKey = ['id'] as const;
  const log: SnitchMessage[] = [];
  const source = createSource(lc, testLogConfig, 'table', columns, primaryKey);
  const snitch = new ThrowingSnitch(
    source.connect([['id', 'asc']]),
    'takeSnitch',
    log,
  );
  const storage = new MemoryStorage();
  const limit = 10;

  const take = new Take(snitch, storage, limit);
  expect(() => [...take.fetch({})]).toThrow('ThrowingSnitch error');
});

test('early return during hydrate', () => {
  const columns = {id: {type: 'string'}, created: {type: 'number'}} as const;
  const primaryKey = ['id'] as const;
  const log: SnitchMessage[] = [];
  const source = createSource(lc, testLogConfig, 'table', columns, primaryKey);
  const sourceRows = [
    {id: 'i1', created: 100},
    {id: 'i2', created: 200},
    {id: 'i3', created: 300},
  ];
  for (const row of sourceRows) {
    consume(source.push({type: 'add', row}));
  }
  const snitch = new Snitch(source.connect([['id', 'asc']]), 'takeSnitch', log);
  const storage = new MemoryStorage();
  const limit = 10;

  const take = new Take(snitch, storage, limit);
  expect(() => {
    let count = 0;
    for (const _ of take.fetch({})) {
      count++;
      if (count > 1) {
        break;
      }
    }
  }).toThrow('Unexpected early return prevented full hydration');
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
    partitionKey: ['issueID'],
  } as const;

  test('limit 0', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
      ],
      limit: 0,
      partitionValues: [['i1'], ['i2']],
    });

    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`{}`);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);

    expect(partitions[1].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [],
      }
    `);
    expect(partitions[1].storage).toMatchInlineSnapshot(`{}`);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('no data', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [],
      limit: 5,
      partitionValues: [['i1'], ['i2']],
    });

    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take","i1"]": {
          "bound": undefined,
          "size": 0,
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);

    expect(partitions[1].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[1].storage).toMatchInlineSnapshot(`
      {
        "["take","i1"]": {
          "bound": undefined,
          "size": 0,
        },
        "["take","i2"]": {
          "bound": undefined,
          "size": 0,
        },
      }
    `);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('less data than limit', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
      ],
      limit: 5,
      partitionValues: [['i0'], ['i1'], ['i2']],
    });

    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i0",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take","i0"]": {
          "bound": undefined,
          "size": 0,
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);

    expect(partitions[1].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[1].storage).toMatchInlineSnapshot(`
      {
        "["take","i0"]": {
          "bound": undefined,
          "size": 0,
        },
        "["take","i1"]": {
          "bound": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
          "size": 3,
        },
        "maxBound": {
          "created": 300,
          "id": "c3",
          "issueID": "i1",
        },
      }
    `);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c1",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "c2",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
        },
      ]
    `);

    expect(partitions[2].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[2].storage).toMatchInlineSnapshot(`
      {
        "["take","i0"]": {
          "bound": undefined,
          "size": 0,
        },
        "["take","i1"]": {
          "bound": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
          "size": 3,
        },
        "["take","i2"]": {
          "bound": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
          },
          "size": 2,
        },
        "maxBound": {
          "created": 500,
          "id": "c5",
          "issueID": "i2",
        },
      }
    `);
    expect(partitions[2].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 400,
            "id": "c4",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
          },
        },
      ]
    `);
  });

  test('data size and limit equal', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
        {id: 'c6', issueID: 'i2', created: 600},
      ],
      limit: 3,
      partitionValues: [['i1'], ['i2']],
    });

    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take","i1"]": {
          "bound": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
          "size": 3,
        },
        "maxBound": {
          "created": 300,
          "id": "c3",
          "issueID": "i1",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c1",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "c2",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
        },
      ]
    `);

    expect(partitions[1].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[1].storage).toMatchInlineSnapshot(`
      {
        "["take","i1"]": {
          "bound": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
          "size": 3,
        },
        "["take","i2"]": {
          "bound": {
            "created": 600,
            "id": "c6",
            "issueID": "i2",
          },
          "size": 3,
        },
        "maxBound": {
          "created": 600,
          "id": "c6",
          "issueID": "i2",
        },
      }
    `);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 400,
            "id": "c4",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 600,
            "id": "c6",
            "issueID": "i2",
          },
        },
      ]
    `);
  });

  test('more data than limit', () => {
    const {partitions} = takeTest({
      ...base,
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
      partitionValues: [['i1'], ['i2']],
    });

    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take","i1"]": {
          "bound": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
          "size": 3,
        },
        "maxBound": {
          "created": 300,
          "id": "c3",
          "issueID": "i1",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c1",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "c2",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
        },
      ]
    `);

    expect(partitions[1].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[1].storage).toMatchInlineSnapshot(`
      {
        "["take","i1"]": {
          "bound": {
            "created": 300,
            "id": "c3",
            "issueID": "i1",
          },
          "size": 3,
        },
        "["take","i2"]": {
          "bound": {
            "created": 600,
            "id": "c6",
            "issueID": "i2",
          },
          "size": 3,
        },
        "maxBound": {
          "created": 600,
          "id": "c6",
          "issueID": "i2",
        },
      }
    `);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 400,
            "id": "c4",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 600,
            "id": "c6",
            "issueID": "i2",
          },
        },
      ]
    `);
  });

  test('compound partition key more data than limit', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 100},
        {id: 'c3', issueID: 'i1', created: 100},
        {id: 'c4', issueID: 'i1', created: 200},
        {id: 'c5', issueID: 'i2', created: 100},
        {id: 'c6', issueID: 'i2', created: 100},
        {id: 'c7', issueID: 'i2', created: 200},
        {id: 'c8', issueID: 'i2', created: 200},
      ],
      limit: 2,
      partitionKey: ['issueID', 'created'],
      partitionValues: [
        ['i1', 100],
        ['i1', 200],
        ['i2', 100],
        ['i2', 200],
      ],
    });

    expect(partitions[0].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 100,
                "issueID": "i1",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 100,
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["take","i1",100]": {
          "bound": {
            "created": 100,
            "id": "c2",
            "issueID": "i1",
          },
          "size": 2,
        },
        "maxBound": {
          "created": 100,
          "id": "c2",
          "issueID": "i1",
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c1",
            "issueID": "i1",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c2",
            "issueID": "i1",
          },
        },
      ]
    `);

    expect(partitions[1].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 200,
                "issueID": "i1",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 200,
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[1].storage).toMatchInlineSnapshot(`
      {
        "["take","i1",100]": {
          "bound": {
            "created": 100,
            "id": "c2",
            "issueID": "i1",
          },
          "size": 2,
        },
        "["take","i1",200]": {
          "bound": {
            "created": 200,
            "id": "c4",
            "issueID": "i1",
          },
          "size": 1,
        },
        "maxBound": {
          "created": 200,
          "id": "c4",
          "issueID": "i1",
        },
      }
    `);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "c4",
            "issueID": "i1",
          },
        },
      ]
    `);

    expect(partitions[2].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 100,
                "issueID": "i2",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 100,
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[2].storage).toMatchInlineSnapshot(`
      {
        "["take","i1",100]": {
          "bound": {
            "created": 100,
            "id": "c2",
            "issueID": "i1",
          },
          "size": 2,
        },
        "["take","i1",200]": {
          "bound": {
            "created": 200,
            "id": "c4",
            "issueID": "i1",
          },
          "size": 1,
        },
        "["take","i2",100]": {
          "bound": {
            "created": 100,
            "id": "c6",
            "issueID": "i2",
          },
          "size": 2,
        },
        "maxBound": {
          "created": 200,
          "id": "c4",
          "issueID": "i1",
        },
      }
    `);
    expect(partitions[2].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c5",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 100,
            "id": "c6",
            "issueID": "i2",
          },
        },
      ]
    `);

    expect(partitions[3].messages).toMatchInlineSnapshot(`
      {
        "fetch": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 200,
                "issueID": "i2",
              },
            },
          ],
        ],
        "hydrate": [
          [
            "takeSnitch",
            "fetch",
            {
              "constraint": {
                "created": 200,
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(partitions[3].storage).toMatchInlineSnapshot(`
      {
        "["take","i1",100]": {
          "bound": {
            "created": 100,
            "id": "c2",
            "issueID": "i1",
          },
          "size": 2,
        },
        "["take","i1",200]": {
          "bound": {
            "created": 200,
            "id": "c4",
            "issueID": "i1",
          },
          "size": 1,
        },
        "["take","i2",100]": {
          "bound": {
            "created": 100,
            "id": "c6",
            "issueID": "i2",
          },
          "size": 2,
        },
        "["take","i2",200]": {
          "bound": {
            "created": 200,
            "id": "c8",
            "issueID": "i2",
          },
          "size": 2,
        },
        "maxBound": {
          "created": 200,
          "id": "c8",
          "issueID": "i2",
        },
      }
    `);
    expect(partitions[3].hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "c7",
            "issueID": "i2",
          },
        },
        {
          "relationships": {},
          "row": {
            "created": 200,
            "id": "c8",
            "issueID": "i2",
          },
        },
      ]
    `);
  });
});

function takeTest(t: TakeTest): TakeTestResults {
  const log: SnitchMessage[] = [];
  const source = createSource(
    lc,
    testLogConfig,
    'table',
    t.columns,
    t.primaryKey,
  );
  for (const row of t.sourceRows) {
    consume(source.push({type: 'add', row}));
  }
  const snitch = new Snitch(
    source.connect(t.sort || [['id', 'asc']]),
    'takeSnitch',
    log,
  );
  const storage = new MemoryStorage();

  const {partitionKey} = t;
  const take = new Take(snitch, storage, t.limit, partitionKey);
  if (t.partitionKey === undefined) {
    assert(t.partitionValues.length === 1);
    assert(t.partitionValues[0] === undefined);
  }
  const results: TakeTestResults = {
    partitions: [],
  };
  for (const partitionValue of t.partitionValues) {
    const partitionResults: PartitionTestResults = {
      messages: {
        hydrate: [],
        fetch: [],
      },
      storage: {},
      hydrate: [],
    };
    results.partitions.push(partitionResults);
    for (const phase of ['hydrate', 'fetch'] as const) {
      log.length = 0;

      const c = new Catch(take);
      const r = c.fetch(
        partitionKey &&
          partitionValue && {
            constraint: Object.fromEntries(
              partitionKey.map((k, i) => [k, partitionValue[i]]),
            ),
          },
      );
      if (phase === 'hydrate') {
        partitionResults.hydrate = r;
      } else {
        phase satisfies 'fetch';
        expect(r).toEqual(partitionResults.hydrate);
      }

      if (phase === 'hydrate') {
        partitionResults.storage = storage.cloneData();
      } else {
        phase satisfies 'fetch';
        expect(storage.cloneData()).toEqual(partitionResults.storage);
      }

      partitionResults.messages[phase] = [...log];
    }
  }
  return results;
}

type TakeTest = {
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sourceRows: Row[];
  sort?: Ordering | undefined;
  limit: number;
  partitionKey: PartitionKey | undefined;
  partitionValues: readonly ([Value, ...Value[]] | undefined)[];
};

type TakeTestResults = {
  partitions: PartitionTestResults[];
};

type PartitionTestResults = {
  messages: {
    hydrate: SnitchMessage[];
    fetch: SnitchMessage[];
  };
  storage: Record<string, JSONValue>;
  hydrate: CaughtNode[];
};
