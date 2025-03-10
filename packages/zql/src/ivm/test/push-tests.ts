import {expect} from 'vitest';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../../zero-schema/src/table-schema.ts';
import {ArrayView} from '../array-view.ts';
import {Catch} from '../catch.ts';
import type {Input} from '../operator.ts';
import type {Source, SourceChange} from '../source.ts';
import type {Format} from '../view.ts';
import {createSource} from './source-factory.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {LogConfig} from '../../../../otel/src/log-options.ts';
import {TestBuilderDelegate} from '../../builder/test-builder-delegate.ts';
import {buildPipeline} from '../../builder/builder.ts';

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

function makeSource(
  tableName: string,
  rows: readonly Row[],
  columns: Readonly<Record<string, SchemaValue>>,
  primaryKeys: PrimaryKey,
): Source {
  const source = createSource(lc, logConfig, tableName, columns, primaryKeys);
  for (const row of rows) {
    source.push({type: 'add', row});
  }
  return source;
}

export type Sources = Record<
  string,
  {
    columns: Record<string, SchemaValue>;
    primaryKeys: PrimaryKey;
  }
>;

export type SourceContents = Readonly<Record<string, readonly Row[]>>;

export type Pushes = [sourceName: string, change: SourceChange][];

export type PushTest = {
  sources: Sources;
  sourceContents: SourceContents;
  ast: AST;
  format: Format;
  pushes: Pushes;
};

export function runPushTest(t: PushTest) {
  function innerTest<T>(makeFinalOutput: (j: Input) => T) {
    const sources: Record<string, Source> = Object.fromEntries(
      Object.entries(t.sources).map(([name, {columns, primaryKeys}]) => [
        name,
        makeSource(name, t.sourceContents[name] ?? [], columns, primaryKeys),
      ]),
    );

    const builderDelegate = new TestBuilderDelegate(sources, true);
    const pipeline = buildPipeline(t.ast, builderDelegate);

    const finalOutput = makeFinalOutput(pipeline);

    builderDelegate.clearLog();

    for (const [name, change] of t.pushes) {
      must(builderDelegate.getSource(name)).push(change);
    }

    return {
      log: builderDelegate.log,
      finalOutput,
      actualStorage: builderDelegate.clonedStorage,
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
