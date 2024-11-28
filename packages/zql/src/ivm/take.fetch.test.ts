import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import type {JSONValue} from '../../../shared/src/json.js';
import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row, Value} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {Catch} from './catch.js';
import {SetOfConstraint} from './constraint.js';
import type {Node} from './data.js';
import {MemoryStorage} from './memory-storage.js';
import {Snitch, type SnitchMessage} from './snitch.js';
import {Take, type PartitionKey} from './take.js';
import {createSource} from './test/source-factory.js';

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
    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();
  });

  test('no data', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [],
      limit: 5,
    });
    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();
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
    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();
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
    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();
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
    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();
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
    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();
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

    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();

    expect(partitions[1].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[1].storage).toMatchInlineSnapshot();
    expect(partitions[1].hydrate).toMatchInlineSnapshot();
  });

  test('no data', () => {
    const {partitions} = takeTest({
      ...base,
      sourceRows: [],
      limit: 5,
      partitionValues: [['i1'], ['i2']],
    });

    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();

    expect(partitions[1].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[1].storage).toMatchInlineSnapshot();
    expect(partitions[1].hydrate).toMatchInlineSnapshot();
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

    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();

    expect(partitions[1].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[1].storage).toMatchInlineSnapshot();
    expect(partitions[1].hydrate).toMatchInlineSnapshot();

    expect(partitions[2].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[2].storage).toMatchInlineSnapshot();
    expect(partitions[2].hydrate).toMatchInlineSnapshot();
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

    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();

    expect(partitions[1].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[1].storage).toMatchInlineSnapshot();
    expect(partitions[1].hydrate).toMatchInlineSnapshot();
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

    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();

    expect(partitions[1].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[1].storage).toMatchInlineSnapshot();
    expect(partitions[1].hydrate).toMatchInlineSnapshot();
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

    expect(partitions[0].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[0].storage).toMatchInlineSnapshot();
    expect(partitions[0].hydrate).toMatchInlineSnapshot();

    expect(partitions[1].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[1].storage).toMatchInlineSnapshot();
    expect(partitions[1].hydrate).toMatchInlineSnapshot();

    expect(partitions[2].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[2].storage).toMatchInlineSnapshot();
    expect(partitions[2].hydrate).toMatchInlineSnapshot();

    expect(partitions[3].fetchMessages).toMatchInlineSnapshot();
    expect(partitions[3].storage).toMatchInlineSnapshot();
    expect(partitions[3].hydrate).toMatchInlineSnapshot();
  });
});

function takeTest(t: TakeTest): TakeTestResults {
  const log: SnitchMessage[] = [];
  const source = createSource('table', t.columns, t.primaryKey);
  for (const row of t.sourceRows) {
    source.push({type: 'add', row});
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
      fetchMessages: [],
      storage: {},
      hydrate: [],
    };
    results.partitions.push(partitionResults);
    for (const [phase, fetchType] of [
      ['hydrate', 'fetch'],
      ['fetch', 'fetch'],
      ['cleanup', 'cleanup'],
    ] as const) {
      log.length = 0;

      const c = new Catch(take);
      const r = c[fetchType](
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
        expect(r).toEqual(partitionResults.hydrate);
      }

      if (phase === 'hydrate') {
        partitionResults.storage = storage.cloneData();
      } else if (phase === 'fetch') {
        expect(storage.cloneData()).toEqual(partitionResults.storage);
      } else {
        phase satisfies 'cleanup';
        expect(storage.cloneData()).toEqual(
          'maxBound' in partitionResults.storage
            ? {maxBound: partitionResults.storage.maxBound}
            : {},
        );
      }

      if (phase === 'hydrate') {
        partitionResults.fetchMessages = [...log];
      } else if (phase === 'fetch') {
        // should be the same as for hydrate
        expect(log).toEqual(partitionResults.fetchMessages);
      } else {
        // For cleanup, the last fetch for any constraint should be a cleanup.
        // Others should be fetch.
        phase satisfies 'cleanup';
        const expectedMessages = [];
        const seen = new SetOfConstraint();
        for (let i = partitionResults.fetchMessages.length - 1; i >= 0; i--) {
          const [name, type, req] = partitionResults.fetchMessages[i];
          expect(type).toSatisfy(t => t === 'fetch' || t === 'cleanup');
          assert(type !== 'push');
          if (!(req.constraint && seen.has(req.constraint))) {
            expectedMessages[i] = [name, 'cleanup', req];
          } else {
            expectedMessages[i] = [name, 'fetch', req];
          }
          req.constraint && seen.add(req.constraint);
        }
        expect(log).toEqual(expectedMessages);
      }
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
  fetchMessages: SnitchMessage[];
  storage: Record<string, JSONValue>;
  hydrate: Node[];
};
