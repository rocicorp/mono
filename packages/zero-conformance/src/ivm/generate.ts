import type {JSONValue} from '../../../shared/src/json.ts';
import type {SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Random} from '../random.ts';
import type {Expr, Op, Program, Push} from './harness.ts';

/** Strings for text columns: orderings differ between UTF-8 and UTF-16. */
export const texts = [
  '',
  'a',
  'A',
  'b',
  'ab',
  'abc',
  'Bug',
  'bug',
  'é',
  'é',
  '\u{1f600}',
  '',
  'z%',
  'a_c',
  '中文',
];

export const numbers = [
  0,
  -0,
  1,
  2,
  2,
  3.5,
  -1,
  2 ** 31,
  2 ** 53 - 1,
  1e21,
  0.1,
];

const issueIDs = ['i1', 'i2', 'i3', 'i4', 'i5', 'i6', 'i7', 'i8'];
const userIDs = ['u1', 'u2', 'u3', 'u4'];
const labelIDs = ['l1', 'l2', 'l3', 'l4'];
const commentIDs = Array.from({length: 12}, (_, i) => `c${i + 1}`);
const revisionIDs = Array.from({length: 6}, (_, i) => `r${i + 1}`);

/** Values that rows of each table and column draw from. */
function columnValue(r: Random, table: string, column: string): JSONValue {
  switch (`${table}.${column}`) {
    case 'issue.ownerId':
      return r.bool(0.2) ? null : r.pick(userIDs);
    case 'issue.closed':
      return r.bool();
    case 'issue.createdAt':
    case 'comment.createdAt':
      return r.pick(numbers);
    case 'comment.issueId':
      return r.bool(0.1) ? 'i-missing' : r.pick(issueIDs);
    case 'comment.authorId':
    case 'revision.authorId':
      return r.pick(userIDs);
    case 'revision.commentId':
      return r.pick(commentIDs);
    case 'issueLabel.issueId':
      return r.pick(issueIDs);
    case 'issueLabel.labelId':
      return r.pick(labelIDs);
    case 'user.metadata':
      return r.bool(0.4)
        ? null
        : {registrar: r.pick(['github', 'google']), email: r.pick(texts)};
    default:
      return r.pick(texts);
  }
}

function idsFor(table: string): string[] {
  switch (table) {
    case 'issue':
      return issueIDs;
    case 'user':
      return userIDs;
    case 'label':
      return labelIDs;
    case 'comment':
      return commentIDs;
    case 'revision':
      return revisionIDs;
    default:
      return [];
  }
}

function makeRow(r: Random, schema: Schema, table: string, id?: string): Row {
  const row: Record<string, JSONValue> = {};
  for (const column of Object.keys(schema.tables[table].columns)) {
    row[column] =
      column === 'id' && id !== undefined ? id : columnValue(r, table, column);
  }
  return row as Row;
}

function pkString(schema: Schema, table: string, row: Row): string {
  return JSON.stringify(schema.tables[table].primaryKey.map(k => row[k]));
}

/** Rows for every table; roughly 70% of each table's ids are present. */
export function makeData(r: Random, schema: Schema): Record<string, Row[]> {
  const data: Record<string, Row[]> = {};
  for (const table of Object.keys(schema.tables)) {
    const rows: Row[] = [];
    if (table === 'issueLabel') {
      const seen = new Set<string>();
      for (let i = 0; i < 12; i++) {
        const row = makeRow(r, schema, table);
        const key = pkString(schema, table, row);
        if (!seen.has(key)) {
          seen.add(key);
          rows.push(row);
        }
      }
    } else {
      for (const id of idsFor(table)) {
        if (r.bool(0.7)) {
          rows.push(makeRow(r, schema, table, id));
        }
      }
    }
    data[table] = rows;
  }
  return data;
}

/** A sequence of valid source changes against `data`. */
export function makePushes(
  r: Random,
  schema: Schema,
  data: Record<string, Row[]>,
  count: number,
): Push[] {
  const current = new Map<string, Map<string, Row>>();
  for (const [table, rows] of Object.entries(data)) {
    current.set(
      table,
      new Map(rows.map(row => [pkString(schema, table, row), row])),
    );
  }
  const tables = Object.keys(schema.tables);
  const pushes: Push[] = [];
  while (pushes.length < count) {
    const table = r.pick(tables);
    const rows = current.get(table)!;
    const kind = r.int(3);
    if (kind === 0) {
      const ids = idsFor(table);
      const candidates =
        table === 'issueLabel'
          ? [makeRow(r, schema, table)]
          : ids.map(id => makeRow(r, schema, table, id));
      const row = candidates.find(c => !rows.has(pkString(schema, table, c)));
      if (!row) {
        continue;
      }
      rows.set(pkString(schema, table, row), row);
      pushes.push({table, type: 'add', row});
    } else if (rows.size > 0) {
      const existing = r.pick([...rows.values()]);
      const key = pkString(schema, table, existing);
      if (kind === 1) {
        rows.delete(key);
        pushes.push({table, type: 'remove', row: existing});
      } else {
        const pk = schema.tables[table].primaryKey;
        const columns = Object.keys(schema.tables[table].columns).filter(
          c => !pk.includes(c),
        );
        if (columns.length === 0) {
          continue;
        }
        const row = {...existing} as Record<string, JSONValue>;
        const n = 1 + r.int(2);
        for (let i = 0; i < n; i++) {
          const column = r.pick(columns);
          row[column] = columnValue(r, table, column);
        }
        rows.set(key, row as Row);
        pushes.push({table, type: 'edit', row: row as Row, oldRow: existing});
      }
    }
  }
  return pushes;
}

const stringOps: SimpleOperator[] = [
  '=',
  '!=',
  '<',
  '>',
  '<=',
  '>=',
  'LIKE',
  'NOT LIKE',
  'ILIKE',
  'NOT ILIKE',
  'IN',
  'NOT IN',
  'IS',
  'IS NOT',
];
const numberOps: SimpleOperator[] = [
  '=',
  '!=',
  '<',
  '>',
  '<=',
  '>=',
  'IN',
  'NOT IN',
];
const likePatterns = [
  '%',
  'a%',
  '%b%',
  'A_C',
  '_',
  '%é%',
  'bug',
  'z\\%',
  '%\u{1f600}',
];

function comparison(
  r: Random,
  schema: Schema,
  table: string,
): [string, SimpleOperator, JSONValue] | undefined {
  const columns = Object.entries(schema.tables[table].columns).filter(
    ([, v]) => v.type !== 'json',
  );
  const [column, value] = r.pick(columns);
  if (value.type === 'boolean') {
    return [column, r.pick(['=', '!='] as const), r.bool()];
  }
  if (value.type === 'number') {
    const op = r.pick(numberOps);
    if (op === 'IN' || op === 'NOT IN') {
      return [column, op, [r.pick(numbers), r.pick(numbers)]];
    }
    return [column, op, r.pick(numbers)];
  }
  const op = r.pick(stringOps);
  const domain = [...idsFor(table), ...texts, ...userIDs, ...issueIDs];
  switch (op) {
    case 'LIKE':
    case 'NOT LIKE':
    case 'ILIKE':
    case 'NOT ILIKE':
      return [column, op, r.pick(likePatterns)];
    case 'IN':
    case 'NOT IN':
      return [column, op, [r.pick(domain), r.pick(domain), r.pick(domain)]];
    case 'IS':
    case 'IS NOT':
      return [column, op, r.bool(0.5) ? null : r.pick(domain)];
    default:
      return [column, op, r.pick(domain)];
  }
}

type Context = {depth: number; inJunction: boolean; inExists: boolean};

function relationshipsOf(schema: Schema, table: string) {
  return Object.entries(schema.relationships[table] ?? {});
}

function existsOp(r: Random, schema: Schema, table: string, ctx: Context) {
  const rels = relationshipsOf(schema, table);
  if (rels.length === 0 || ctx.depth > 2) {
    return undefined;
  }
  const [rel, hops] = r.pick(rels);
  const dest = hops.at(-1)!.destSchema;
  const sub = r.bool(0.6)
    ? subOps(r, schema, dest, {
        depth: ctx.depth + 1,
        inJunction: hops.length > 1,
        inExists: true,
      })
    : undefined;
  return {
    rel,
    sub,
    flip: r.bool(0.3) ? r.bool() : undefined,
    scalar: r.bool(0.1) ? true : undefined,
  };
}

function expr(
  r: Random,
  schema: Schema,
  table: string,
  ctx: Context,
  depth = 0,
  // The client rejects NOT EXISTS, so a negated expression has no subqueries.
  negated = false,
): Expr {
  const k = r.int(depth > 1 ? 2 : 6);
  if (k <= 1) {
    const c = comparison(r, schema, table);
    if (c) {
      return {cmp: c};
    }
  }
  if (k === 5) {
    return {not: expr(r, schema, table, ctx, depth + 1, true)};
  }
  if (k === 2 && !negated) {
    const e = existsOp(r, schema, table, ctx);
    if (e) {
      return {exists: e.rel, sub: e.sub, flip: e.flip, scalar: e.scalar};
    }
  }
  const n = 1 + r.int(3);
  const parts = Array.from({length: n}, () =>
    expr(r, schema, table, ctx, depth + 1, negated),
  );
  return r.bool() ? {and: parts} : {or: parts};
}

/** Ops for a subquery, avoiding what the builder rejects there. */
function subOps(r: Random, schema: Schema, table: string, ctx: Context): Op[] {
  const ops: Op[] = [];
  const n = r.int(3);
  for (let i = 0; i < n; i++) {
    ops.push(...randomOps(r, schema, table, ctx, 1));
  }
  return ops;
}

function randomOps(
  r: Random,
  schema: Schema,
  table: string,
  ctx: Context,
  count: number,
  data?: Record<string, Row[]>,
): Op[] {
  const ops: Op[] = [];
  for (let i = 0; i < count; i++) {
    const k = r.int(10);
    if (k <= 2) {
      const c = comparison(r, schema, table);
      if (c) {
        ops.push(
          c[1] === '=' && r.bool()
            ? {op: 'where', field: c[0], value: c[2]}
            : {op: 'where', field: c[0], cmp: c[1], value: c[2]},
        );
      }
    } else if (k === 3) {
      ops.push({op: 'whereExpr', expr: expr(r, schema, table, ctx)});
    } else if (k === 4) {
      const e = existsOp(r, schema, table, ctx);
      if (e) {
        ops.push({op: 'whereExists', ...e});
      }
    } else if (k === 5 && !ctx.inExists && ctx.depth <= 2) {
      const rels = relationshipsOf(schema, table);
      if (rels.length > 0) {
        const [rel, hops] = r.pick(rels);
        const dest = hops.at(-1)!.destSchema;
        ops.push({
          op: 'related',
          rel,
          sub: r.bool(0.7)
            ? subOps(r, schema, dest, {
                depth: ctx.depth + 1,
                inJunction: hops.length > 1,
                inExists: false,
              })
            : undefined,
        });
      }
    } else if (k === 6 && !ctx.inJunction) {
      const columns = Object.entries(schema.tables[table].columns)
        .filter(([, v]) => v.type !== 'json')
        .map(([c]) => c);
      ops.push({
        op: 'orderBy',
        field: r.pick(columns),
        dir: r.pick(['asc', 'desc']),
      });
    } else if (k === 7 && !ctx.inJunction) {
      ops.push({op: 'limit', n: r.pick([0, 1, 2, 3, 5])});
    } else if (k === 8 && !ctx.inJunction && ctx.depth === 0 && data) {
      const rows = data[table];
      if (rows.length > 0) {
        ops.push({
          op: 'start',
          row: r.pick(rows),
          inclusive: r.bool(0.3) ? r.bool() : undefined,
        });
      }
    } else if (k === 9 && !ctx.inJunction && !ctx.inExists && r.bool(0.3)) {
      ops.push({op: 'one'});
    }
  }
  return ops;
}

/** A random query program over `schema`. */
export function makeProgram(
  r: Random,
  schema: Schema,
  data: Record<string, Row[]>,
): Program {
  const table = r.pick(['issue', 'issue', 'comment', 'user', 'label']);
  return {
    table,
    ops: randomOps(
      r,
      schema,
      table,
      {depth: 0, inJunction: false, inExists: false},
      1 + r.int(5),
      data,
    ),
  };
}
