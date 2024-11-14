import {assert, expect, test} from 'vitest';
import {Snitch, type SnitchMessage} from '../snitch.js';
import {createPrimaryKeySetStorageKey, Join} from '../join.js';
import {MemoryStorage} from '../memory-storage.js';
import {Catch} from '../catch.js';
import type {SchemaValue} from '../../../../zero-schema/src/table-schema.js';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.js';
import type {Row} from '../../../../zero-protocol/src/data.js';
import type {Ordering} from '../../../../zero-protocol/src/ast.js';
import type {SourceChange} from '../source.js';
import type {NormalizedValue} from '../data.js';
import type {Change} from '../change.js';
import {MemorySource} from '../memory-source.js';
import type {Format} from '../view.js';
import type {Storage, Input, Operator} from '../operator.js';
import {must} from '../../../../shared/src/must.js';
import type {JSONObject} from '../../../../shared/src/json.js';
import {ArrayView} from '../array-view.js';

export function pushTest(t: PushTest) {
  test(t.name, () => {
    assert(t.sources.length > 0);
    assert(t.joins.length === t.sources.length - 1);

    const log: SnitchMessage[] = [];

    const sources = t.sources.map((rows, i) =>
      makeSource(
        rows,
        t.sorts?.[i] ?? [['id', 'asc']],
        t.columns[i],
        t.primaryKeys[i],
        String(i),
        log,
      ),
    );

    const joins: {
      join: Join;
      storage: MemoryStorage;
    }[] = [];
    // Although we tend to think of the joins from left to right, we need to
    // build them from right to left.
    for (let i = t.joins.length - 1; i >= 0; i--) {
      const info = t.joins[i];
      const parent = sources[i].snitch;
      const child =
        i === t.joins.length - 1 ? sources[i + 1].snitch : joins[i + 1].join;
      const storage = new MemoryStorage();
      const join = new Join({
        parent,
        child,
        storage,
        ...info,
        hidden: false,
      });
      joins[i] = {
        join,
        storage,
      };
    }

    // By convention we put them in the test bottom up. Why? Easier to think
    // left-to-right.
    const finalJoin = joins[0];
    const c = new Catch(finalJoin.join);

    c.fetch();
    log.length = 0;

    for (const [sourceIndex, change] of t.pushes) {
      sources[sourceIndex].source.push(change);
    }

    for (const [i, j] of joins.entries()) {
      const {storage} = j;
      const expectedStorageKeys = t.expectedPrimaryKeySetStorageKeys[i];
      const expectedStorage: Record<string, boolean> = {};
      for (const k of expectedStorageKeys) {
        expectedStorage[createPrimaryKeySetStorageKey(k)] = true;
      }
      expect(storage.cloneData()).toEqual(expectedStorage);
    }

    expect(log).toEqual(t.expectedLog);
    expect(c.pushes).toEqual(t.expectedOutput);
  });
}

type PushTest = {
  name: string;
  columns: readonly Record<string, SchemaValue>[];
  primaryKeys: readonly PrimaryKey[];
  sources: Row[][];
  sorts?: Record<number, Ordering> | undefined;
  joins: readonly {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  }[];
  pushes: [sourceIndex: number, change: SourceChange][];
  expectedLog: SnitchMessage[];
  expectedPrimaryKeySetStorageKeys: NormalizedValue[][][];
  expectedOutput: Change[];
};

function makeSource(
  rows: Row[],
  ordering: Ordering,
  columns: Record<string, SchemaValue>,
  primaryKeys: PrimaryKey,
  snitchName: string,
  log: SnitchMessage[],
): {source: MemorySource; snitch: Snitch} {
  const source = new MemorySource('test', columns, primaryKeys);
  for (const row of rows) {
    source.push({type: 'add', row});
  }
  const snitch = new Snitch(source.connect(ordering), snitchName, log);
  return {
    source,
    snitch,
  };
}

export type Sources = Record<
  string,
  {
    columns: Record<string, SchemaValue>;
    primaryKeys: PrimaryKey;
    sorts: Ordering;
    rows: Row[];
  }
>;

export type Joins = Record<
  string,
  {
    parentKey: string;
    parentSource: string;
    childKey: string;
    childSource: string;
    relationshipName: string;
  }
>;

export type Pushes = [sourceName: string, change: SourceChange][];

export type NewPushTest = {
  sources: Sources;
  format: Format;
  joins: Joins;
  pushes: Pushes;
  addPostJoinsOperator?:
    | ((i: Input, storage: Storage) => {name: string; op: Operator})
    | undefined;
};

export function runJoinTest(t: NewPushTest) {
  function innerTest<T>(makeFinalOutput: (j: Input) => T) {
    const log: SnitchMessage[] = [];

    const sources: Record<
      string,
      {
        source: MemorySource;
        snitch: Snitch;
      }
    > = Object.fromEntries(
      Object.entries(t.sources).map(
        ([name, {columns, primaryKeys, sorts, rows}]) => [
          name,
          makeSource(rows, sorts, columns, primaryKeys, name, log),
        ],
      ),
    );

    const joins: Record<
      string,
      {
        join: Join;
        snitch: Snitch;
      }
    > = {};
    const storage: Record<string, MemoryStorage> = {};
    let last;
    for (const [name, info] of Object.entries(t.joins)) {
      const joinStorage = new MemoryStorage();
      const join = new Join({
        parent: (sources[info.parentSource] ?? joins[info.parentSource]).snitch,
        parentKey: info.parentKey,
        child: (sources[info.childSource] ?? joins[info.childSource]).snitch,
        childKey: info.childKey,
        storage: joinStorage,
        relationshipName: info.relationshipName,
        hidden: false,
      });
      const snitch = new Snitch(join, name, log);
      last = joins[name] = {
        join,
        snitch,
      };
      storage[name] = joinStorage;
    }

    // By convention we put them in the test bottom up. Why? Easier to think
    // left-to-right.

    let lastSnitch: Snitch;
    if (t.addPostJoinsOperator !== undefined) {
      const postOpStorage = new MemoryStorage();
      const {name, op} = t.addPostJoinsOperator(
        must(last).snitch,
        postOpStorage,
      );
      storage[name] = postOpStorage;
      lastSnitch = new Snitch(op, name, log);
    } else {
      lastSnitch = must(last).snitch;
    }

    const finalOutput = makeFinalOutput(lastSnitch);

    log.length = 0;

    for (const [sourceIndex, change] of t.pushes) {
      sources[sourceIndex].source.push(change);
    }

    const actualStorage: Record<string, JSONObject> = {};
    for (const [name, s] of Object.entries(storage)) {
      actualStorage[name] = s.cloneData();
    }

    return {
      log,
      finalOutput,
      actualStorage,
    };
  }

  const {
    log,
    finalOutput: catchOp,
    actualStorage,
  } = innerTest(j => {
    const c = new Catch(j);
    c.fetch();
    return c;
  });

  let data;
  const {
    log: log2,
    finalOutput: view,
    actualStorage: actualStorage2,
  } = innerTest(j => {
    const view = new ArrayView(j, t.format);
    data = view.data;
    return view;
  });

  view.addListener(v => {
    data = v;
  });

  expect(log).toEqual(log2);
  expect(actualStorage).toEqual(actualStorage2);

  view.flush();
  return {
    log,
    actualStorage,
    pushes: catchOp.pushes,
    data,
  };
}
