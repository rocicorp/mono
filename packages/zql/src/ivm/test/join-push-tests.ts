import {expect} from 'vitest';
import type {JSONObject} from '../../../../shared/src/json.js';
import {must} from '../../../../shared/src/must.js';
import type {CompoundKey, Ordering} from '../../../../zero-protocol/src/ast.js';
import type {Row} from '../../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../../zero-schema/src/table-schema.js';
import {ArrayView} from '../array-view.js';
import {Catch} from '../catch.js';
import {Join} from '../join.js';
import {MemoryStorage} from '../memory-storage.js';
import type {Input, Operator, Storage} from '../operator.js';
import {Snitch, type SnitchMessage} from '../snitch.js';
import type {Source, SourceChange} from '../source.js';
import type {Format} from '../view.js';
import {createSource} from './source-factory.js';

function makeSource(
  rows: Row[],
  ordering: Ordering,
  columns: Record<string, SchemaValue>,
  primaryKeys: PrimaryKey,
  snitchName: string,
  log: SnitchMessage[],
): {source: Source; snitch: Snitch} {
  const source = createSource('test', columns, primaryKeys);
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
  }
>;

export type SourceContents = Record<string, Row[]>;

export type Joins = Record<
  string,
  {
    parentKey: CompoundKey;
    parentSource: string;
    childKey: CompoundKey;
    childSource: string;
    relationshipName: string;
  }
>;

export type Pushes = [sourceName: string, change: SourceChange][];

export type NewPushTest = {
  sources: Sources;
  sourceContents: SourceContents;
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
        source: Source;
        snitch: Snitch;
      }
    > = Object.fromEntries(
      Object.entries(t.sources).map(([name, {columns, primaryKeys, sorts}]) => [
        name,
        makeSource(
          t.sourceContents[name] ?? [],
          sorts,
          columns,
          primaryKeys,
          name,
          log,
        ),
      ]),
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
        system: 'client',
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
