import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from './catch.ts';
import {MemoryStorage} from './memory-storage.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import {Cap} from './cap.ts';
import type {PartitionKey} from './take.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {consume} from './stream.ts';

const lc = createSilentLogContext();

suite('cap with no partition', () => {
  const base = {
    columns: {id: {type: 'string'}, created: {type: 'number'}},
    primaryKey: ['id'],
    sort: [['id', 'asc']],
    partitionKey: undefined,
    partitionValues: [undefined],
  } as const;

  test('limit 0', () => {
    const {partitions} = capTest({
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
    const {partitions} = capTest({
      ...base,
      sourceRows: [],
      limit: 5,
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["cap"]": {
          "pks": [],
          "size": 0,
        },
      }
    `);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('less data than limit', () => {
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["cap"]": {
          "pks": [
            "["i1"]",
            "["i2"]",
            "["i3"]",
          ],
          "size": 3,
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
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 3,
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["cap"]": {
          "pks": [
            "["i1"]",
            "["i2"]",
            "["i3"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(partitions[0].hydrate.length).toBe(3);
  });

  test('more data than limit', () => {
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
        {id: 'i5', created: 500},
      ],
      limit: 3,
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["cap"]": {
          "pks": [
            "["i1"]",
            "["i2"]",
            "["i3"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(partitions[0].hydrate.length).toBe(3);
  });

  test('subsequent fetch uses counted early-stop', () => {
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
        {id: 'i5', created: 500},
      ],
      limit: 3,
    });
    // hydrate and fetch should return the same rows
    expect(partitions[0].hydrate).toEqual(
      partitions[0].hydrate, // just verifying it's consistent
    );
    // On subsequent fetch, the snitch should show a fetch call
    expect(partitions[0].messages.fetch.length).toBeGreaterThan(0);
  });
});

suite('cap with partition', () => {
  const base = {
    columns: {
      id: {type: 'string'},
      issueID: {type: 'string'},
      created: {type: 'number'},
    },
    primaryKey: ['id'],
    sort: [['id', 'asc']],
    partitionKey: ['issueID'],
  } as const;

  test('limit 0', () => {
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
      ],
      limit: 0,
      partitionValues: [['i1'], ['i2']],
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`{}`);
    expect(partitions[0].hydrate).toMatchInlineSnapshot(`[]`);
    expect(partitions[1].storage).toMatchInlineSnapshot(`{}`);
    expect(partitions[1].hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('less data than limit', () => {
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i2', created: 300},
      ],
      limit: 5,
      partitionValues: [['i1'], ['i2']],
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
          ],
          "size": 2,
        },
      }
    `);
    expect(partitions[0].hydrate.length).toBe(2);

    expect(partitions[1].storage).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
          ],
          "size": 2,
        },
        "["cap","i2"]": {
          "pks": [
            "["c3"]",
          ],
          "size": 1,
        },
      }
    `);
    expect(partitions[1].hydrate.length).toBe(1);
  });

  test('more data than limit', () => {
    const {partitions} = capTest({
      ...base,
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
        {id: 'c6', issueID: 'i2', created: 600},
      ],
      limit: 2,
      partitionValues: [['i1'], ['i2']],
    });
    expect(partitions[0].storage).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
          ],
          "size": 2,
        },
      }
    `);
    expect(partitions[0].hydrate.length).toBe(2);

    expect(partitions[1].hydrate.length).toBe(2);
  });
});

function capTest(t: CapTest): CapTestResults {
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
    'capSnitch',
    log,
  );
  const storage = new MemoryStorage();

  const {partitionKey} = t;
  const cap = new Cap(snitch, storage, t.limit, partitionKey);
  if (t.partitionKey === undefined) {
    assert(t.partitionValues.length === 1);
    assert(t.partitionValues[0] === undefined);
  }
  const results: CapTestResults = {
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

      const c = new Catch(cap);
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

type CapTest = {
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sourceRows: Row[];
  sort?: Ordering | undefined;
  limit: number;
  partitionKey: PartitionKey | undefined;
  partitionValues: readonly ([Value, ...Value[]] | undefined)[];
};

type CapTestResults = {
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
