import {readFile} from 'node:fs/promises';
import {DatabaseSync} from 'node:sqlite';
import {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import type postgres from 'postgres';
import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';
import {quoteIdentifier, quoteTable} from './backfill.ts';

/**
 * Which rows of a Zero replica to copy into the local upstream database.
 * Tables not listed follow `default`. A `where` clause is SQLite, evaluated
 * against the replica; `{{users}}` becomes the list of replayed user IDs.
 */
export type ReplicaSeedPlan = {
  readonly default?: 'copy' | 'skip' | undefined;
  readonly tables?:
    | Readonly<Record<string, 'copy' | 'skip' | {readonly where: string}>>
    | undefined;
};

export async function loadReplicaSeedPlan(
  path: string | undefined,
): Promise<ReplicaSeedPlan> {
  return path === undefined
    ? {}
    : (JSON.parse(await readFile(path, 'utf8')) as ReplicaSeedPlan);
}

type ColumnType = ClientSchema['tables'][string]['columns'][string]['type'];

/**
 * Copies the tables and columns that the client schema, the replica and the
 * already-created upstream tables have in common. Columns the replica lacks
 * stay NULL; tables it lacks stay empty. Values are converted from the
 * replica's storage (booleans as 0/1, JSON as text) by the client type.
 */
export async function copyFromReplica(options: {
  readonly sql: postgres.Sql;
  readonly replicaFile: string;
  readonly plan: ReplicaSeedPlan;
  readonly clientSchema: ClientSchema;
  readonly userIDs: readonly string[];
  readonly log: (message: string) => void;
}): Promise<void> {
  const {sql, plan, clientSchema, log} = options;
  const replica = new DatabaseSync(options.replicaFile, {readOnly: true});
  try {
    const users = `(${options.userIDs.map(sqliteLiteral).join(', ')})`;
    const replicaTables = new Set(
      replica
        .prepare(`SELECT name FROM sqlite_master WHERE type = 'table'`)
        .all()
        .map(r => String(r.name)),
    );
    for (const [table, spec] of Object.entries(clientSchema.tables)) {
      const rule = plan.tables?.[table] ?? plan.default ?? 'copy';
      if (rule === 'skip') {
        log(`replica seed: ${table} skipped by the plan`);
        continue;
      }
      if (!replicaTables.has(table)) {
        log(`replica seed: ${table} is not in the replica; left empty`);
        continue;
      }
      const replicaColumns = new Set(
        replica
          .prepare(`PRAGMA table_info(${sqliteIdentifier(table)})`)
          .all()
          .map(r => String(r.name)),
      );
      const upstreamColumns = await tableColumns(sql, table);
      const columns = Object.keys(spec.columns).filter(
        c => replicaColumns.has(c) && upstreamColumns.has(c),
      );
      const types = columns.map(c => spec.columns[c].type);
      const where =
        typeof rule === 'object'
          ? ` WHERE ${rule.where.replaceAll('{{users}}', users)}`
          : '';
      const select = replica.prepare(
        `SELECT ${columns.map(sqliteIdentifier).join(', ')} FROM ${sqliteIdentifier(table)}${where}`,
      );
      select.setReturnArrays(true);
      const started = Date.now();
      let rows = 0;
      const writable = await sql
        .unsafe(
          `COPY ${quoteTable(table)} (${columns.map(quoteIdentifier).join(', ')}) FROM STDIN WITH (FORMAT csv)`,
        )
        .writable();
      await pipeline(
        Readable.from(
          csvBatches(select.iterate() as Iterable<unknown[]>, types, n => {
            rows += n;
          }),
        ),
        writable,
      );
      const seconds = (Date.now() - started) / 1000;
      log(
        `replica seed: ${table} ${rows} rows in ${seconds.toFixed(1)}s` +
          (columns.length < Object.keys(spec.columns).length
            ? ` (missing columns: ${Object.keys(spec.columns)
                .filter(c => !columns.includes(c))
                .join(', ')})`
            : ''),
      );
    }
  } finally {
    replica.close();
  }
}

/** Yields CSV text in chunks of about a megabyte. */
export function* csvBatches(
  rows: Iterable<readonly unknown[]>,
  types: readonly ColumnType[],
  onRows: (n: number) => void,
): Generator<string> {
  let chunk = '';
  let n = 0;
  for (const row of rows) {
    let line = '';
    for (let i = 0; i < types.length; i++) {
      if (i > 0) {
        line += ',';
      }
      line += csvField(row[i], types[i]);
    }
    chunk += line + '\n';
    n++;
    if (chunk.length >= 1 << 20) {
      onRows(n);
      n = 0;
      yield chunk;
      chunk = '';
    }
  }
  onRows(n);
  if (chunk.length > 0) {
    yield chunk;
  }
}

export function csvField(value: unknown, type: ColumnType): string {
  if (value === null || value === undefined) {
    return '';
  }
  switch (type) {
    case 'boolean':
      return value === 0 || value === false || value === '0' ? 'f' : 't';
    case 'number':
      return String(value);
    default: {
      const text =
        value instanceof Uint8Array
          ? Buffer.from(value).toString('utf8')
          : String(value);
      return `"${text.replaceAll('"', '""')}"`;
    }
  }
}

async function tableColumns(
  sql: postgres.Sql,
  table: string,
): Promise<Set<string>> {
  const [schema, name] = table.includes('.')
    ? table.split('.', 2)
    : ['public', table];
  const rows = await sql<{column: string}[]>`
    SELECT column_name AS "column" FROM information_schema.columns
     WHERE table_schema = ${schema} AND table_name = ${name}`;
  return new Set(rows.map(r => r.column));
}

function sqliteIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

function sqliteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}
