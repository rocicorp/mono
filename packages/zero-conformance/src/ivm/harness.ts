import type {JSONValue} from '../../../shared/src/json.ts';
import type {AST, SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../../zql/src/builder/test-builder-delegate.ts';
import {ArrayView} from '../../../zql/src/ivm/array-view.ts';
import type {Change} from '../../../zql/src/ivm/change.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import type {
  FetchRequest,
  Input,
  InputBase,
  Operator,
  Output,
} from '../../../zql/src/ivm/operator.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../zql/src/ivm/source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import {asQueryImpl, newQuery} from '../../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';

/**
 * A query built through the builder API, written as data so the Swift
 * builder can replay it.
 */
export type Program = {table: string; ops: Op[]};

export type Op =
  | {op: 'where'; field: string; cmp?: SimpleOperator; value: JSONValue}
  | {op: 'whereExpr'; expr: Expr}
  | {
      op: 'whereExists';
      rel: string;
      sub?: Op[] | undefined;
      flip?: boolean | undefined;
      scalar?: boolean | undefined;
    }
  | {op: 'related'; rel: string; sub?: Op[] | undefined}
  | {op: 'orderBy'; field: string; dir: 'asc' | 'desc'}
  | {op: 'limit'; n: number}
  | {op: 'start'; row: Row; inclusive?: boolean | undefined}
  | {op: 'one'};

export type Expr =
  | {cmp: [field: string, op: SimpleOperator, value: JSONValue]}
  | {and: Expr[]}
  | {or: Expr[]}
  | {not: Expr}
  | {
      exists: string;
      sub?: Op[] | undefined;
      flip?: boolean | undefined;
      scalar?: boolean | undefined;
    };

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
type AnyEB = any;

function applyOps(q: AnyQuery, ops: readonly Op[]): AnyQuery {
  for (const op of ops) {
    q = applyOp(q, op);
  }
  return q;
}

function sub(ops: Op[] | undefined) {
  return ops ? (q: AnyQuery) => applyOps(q, ops) : undefined;
}

function existsOptions(o: {
  flip?: boolean | undefined;
  scalar?: boolean | undefined;
}) {
  if (o.flip === undefined && o.scalar === undefined) {
    return undefined;
  }
  return {
    ...(o.flip !== undefined ? {flip: o.flip} : {}),
    ...(o.scalar !== undefined ? {scalar: o.scalar} : {}),
  };
}

function applyOp(q: AnyQuery, op: Op): AnyQuery {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const query = q as any;
  switch (op.op) {
    case 'where':
      return op.cmp === undefined
        ? query.where(op.field, op.value)
        : query.where(op.field, op.cmp, op.value);
    case 'whereExpr':
      return query.where((eb: AnyEB) => buildExpr(eb, op.expr));
    case 'whereExists': {
      const cb = sub(op.sub);
      const options = existsOptions(op);
      if (cb) {
        return query.whereExists(op.rel, cb, options);
      }
      return options
        ? query.whereExists(op.rel, options)
        : query.whereExists(op.rel);
    }
    case 'related': {
      const cb = sub(op.sub);
      return cb ? query.related(op.rel, cb) : query.related(op.rel);
    }
    case 'orderBy':
      return query.orderBy(op.field, op.dir);
    case 'limit':
      return query.limit(op.n);
    case 'start':
      return op.inclusive === undefined
        ? query.start(op.row)
        : query.start(op.row, {inclusive: op.inclusive});
    case 'one':
      return query.one();
  }
}

function buildExpr(eb: AnyEB, expr: Expr): unknown {
  if ('cmp' in expr) {
    return eb.cmp(expr.cmp[0], expr.cmp[1], expr.cmp[2]);
  }
  if ('and' in expr) {
    return eb.and(...expr.and.map(e => buildExpr(eb, e)));
  }
  if ('or' in expr) {
    return eb.or(...expr.or.map(e => buildExpr(eb, e)));
  }
  if ('not' in expr) {
    return eb.not(buildExpr(eb, expr.not));
  }
  const cb = sub(expr.sub);
  return eb.exists(expr.exists, cb, existsOptions(expr));
}

/** Builds `program` with the JS query builder. */
export function buildProgram(
  schema: Schema,
  program: Program,
): {ast: AST; format: Format} {
  const q = applyOps(newQuery(schema, program.table) as AnyQuery, program.ops);
  const impl = asQueryImpl(q);
  return {ast: impl.ast, format: impl.format};
}

/** The client schema as JSON, with column order spelled out. */
export function schemaJSON(schema: Schema): JSONValue {
  const tables: Record<string, JSONValue> = {};
  for (const [name, table] of Object.entries(schema.tables)) {
    const columns: Record<string, JSONValue> = {};
    for (const [column, value] of Object.entries(table.columns)) {
      columns[column] = {
        type: value.type,
        ...(value.optional ? {optional: true} : {}),
        ...(value.serverName ? {serverName: value.serverName} : {}),
      };
    }
    tables[name] = {
      name: table.name,
      ...(table.serverName ? {serverName: table.serverName} : {}),
      columns,
      columnOrder: Object.keys(table.columns),
      primaryKey: [...table.primaryKey],
    };
  }
  const relationships: Record<string, JSONValue> = {};
  for (const [table, rels] of Object.entries(schema.relationships)) {
    const out: Record<string, JSONValue> = {};
    for (const [name, hops] of Object.entries(rels)) {
      out[name] = hops.map(h => ({
        sourceField: [...h.sourceField],
        destField: [...h.destField],
        destSchema: h.destSchema,
        cardinality: h.cardinality,
      }));
    }
    relationships[table] = out;
  }
  return {tables, relationships};
}

export type Push = {
  table: string;
  type: 'add' | 'remove' | 'edit';
  row: Row;
  oldRow?: Row | undefined;
};

/** A change as the view received it, without relationships. */
export type RecordedChange =
  | {type: 'add' | 'remove'; row: Row}
  | {type: 'edit'; row: Row; oldRow: Row}
  | {
      type: 'child';
      row: Row;
      child: {relationshipName: string; change: RecordedChange};
    };

function record(change: Change): RecordedChange {
  switch (change[0]) {
    case 0:
      return {type: 'add', row: change[1].row};
    case 1:
      return {type: 'remove', row: change[1].row};
    case 2:
      return {type: 'edit', row: change[1].row, oldRow: change[2].row};
    case 3:
      return {
        type: 'child',
        row: change[1].row,
        child: {
          relationshipName: change[2].relationshipName,
          change: record(change[2].change),
        },
      };
  }
}

/** Records what the pipeline pushes to the view. */
class Recorder implements Operator {
  readonly #input: Input;
  #output: Output | undefined;
  readonly changes: RecordedChange[] = [];

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema() {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  fetch(req: FetchRequest) {
    return this.#input.fetch(req);
  }

  *push(change: Change, _pusher: InputBase) {
    this.changes.push(record(change));
    yield* this.#output!.push(change, this);
  }

  *reconcile(_pusher: InputBase) {
    if (this.#output?.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }
}

export type Step = {changes: RecordedChange[]; data: JSONValue};

export type QueryResult = {error: string} | {initial: JSONValue; steps: Step[]};

function plain(data: unknown): JSONValue {
  // Drops the view's symbol keys and undefined singular relationships.
  return JSON.parse(JSON.stringify(data ?? null));
}

/**
 * Materializes `ast` over sources loaded with `data`, applies `pushes` one
 * by one, and records the view after each.
 */
export function runQuery(
  schema: Schema,
  data: Record<string, Row[]>,
  ast: AST,
  format: Format,
  pushes: readonly Push[],
): QueryResult {
  const sources: Record<string, MemorySource> = {};
  for (const [name, table] of Object.entries(schema.tables)) {
    const source = new MemorySource(name, table.columns, table.primaryKey);
    for (const row of data[name] ?? []) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  const delegate = new TestBuilderDelegate(sources);
  let recorder: Recorder;
  let view: ArrayView<never>;
  try {
    const input = buildPipeline(ast, delegate, 'q');
    recorder = new Recorder(input);
    view = new ArrayView(recorder, format, true, () => {});
  } catch (e) {
    return {error: String(e instanceof Error ? e.message : e)};
  }
  const initial = plain(view.data);
  const steps: Step[] = [];
  for (const push of pushes) {
    recorder.changes.length = 0;
    const change =
      push.type === 'edit'
        ? makeSourceChangeEdit(push.row, push.oldRow!)
        : push.type === 'add'
          ? makeSourceChangeAdd(push.row)
          : makeSourceChangeRemove(push.row);
    try {
      consume(sources[push.table].push(change));
    } catch (e) {
      return {error: String(e instanceof Error ? e.message : e)};
    }
    view.flush();
    steps.push({changes: [...recorder.changes], data: plain(view.data)});
  }
  return {initial, steps};
}
