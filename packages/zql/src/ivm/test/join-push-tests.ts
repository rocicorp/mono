import {expect} from 'vitest';
import type {JSONObject} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {CompoundKey, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../../zero-schema/src/table-schema.ts';
import {ArrayView} from '../array-view.ts';
import {Catch} from '../catch.ts';
import {Join} from '../join.ts';
import {MemoryStorage} from '../memory-storage.ts';
import type {Input, Operator, Storage} from '../operator.ts';
import {Snitch, type SnitchMessage} from '../snitch.ts';
import type {Source, SourceChange} from '../source.ts';
import type {Format} from '../view.ts';
import {createSource} from './source-factory.ts';

function makeSource(
  rows: readonly Row[],
  ordering: Ordering,
  columns: Readonly<Record<string, SchemaValue>>,
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

export type SourceContents = Readonly<Record<string, readonly Row[]>>;

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
    | undefined
    | ((i: Input, storage: Storage) => {name: string; op: Operator})[];
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

    let lastSnitch: Snitch | undefined;
    if (t.addPostJoinsOperator !== undefined) {
      const addPostJoinsOperators = Array.isArray(t.addPostJoinsOperator)
        ? t.addPostJoinsOperator
        : [t.addPostJoinsOperator];
      for (let i = 0; i < addPostJoinsOperators.length; i++) {
        const postOpStorage = new MemoryStorage();
        const {name, op} = addPostJoinsOperators[i](
          must(last).snitch,
          postOpStorage,
        );
        storage[name] = postOpStorage;
        last = {
          op,
          snitch: new Snitch(op, name, log),
        };
        if (i === addPostJoinsOperators.length - 1) {
          lastSnitch = last.snitch;
        }
      }
    } else {
      lastSnitch = must(last).snitch;
    }

    const finalOutput = makeFinalOutput(must(lastSnitch));

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
