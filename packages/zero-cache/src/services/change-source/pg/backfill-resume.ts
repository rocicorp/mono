import {must} from '../../../../../shared/src/must.ts';
import * as PostgresTypeClass from '../../../db/postgres-type-class-enum.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {
  BOOL,
  INT2,
  INT4,
  INT8,
  TEXT,
  UUID,
  VARCHAR,
} from '../../../types/pg-types.ts';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.ts';
import {id} from '../../../types/sql.ts';

/**
 * The subset of a column spec needed to decide whether the column can
 * participate in a resumable row key, and to render its values as SQL
 * literals.
 *
 * `collationIsDeterministic` is not part of {@link PublishedTableSpec} (it is
 * stripped when the published schema is canonicalized); it is fetched
 * separately with {@link getKeyCollations}.
 */
export type ResumeColumnSpec = {
  readonly typeOID: number;
  readonly pgTypeClass?: string | null | undefined;
  readonly elemPgTypeClass?: string | null | undefined;
  readonly collationIsDeterministic?: boolean | null | undefined;
};

/**
 * A **mark** is the Postgres text form of a row key's values, in
 * `relation.rowKey.columns` order. It is computed by the change source (which
 * knows the column types), stored and returned opaquely by subscribers, and
 * compared only for equality outside of Postgres.
 */
export type Mark = readonly string[];

const INT_TYPES: ReadonlySet<number> = new Set([INT2, INT4, INT8]);
const TEXT_TYPES: ReadonlySet<number> = new Set([TEXT, VARCHAR]);

const INT_RE = /^-?[0-9]+$/;
const UUID_RE =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

/**
 * Whether a single column's values have an exact text form whose literal is
 * trivially safe to inline into a `COPY (query) TO STDOUT` (which accepts no
 * bind parameters).
 *
 * Types whose text form is lossy (`numeric`, the timestamp family, floats) and
 * columns with a non-deterministic collation are excluded. Excluding the latter
 * is conservatism rather than necessity: a unique index under a
 * non-deterministic collation enforces uniqueness by the same equality that
 * orders it.
 */
export function isResumableColumn(spec: ResumeColumnSpec): boolean {
  if (spec.elemPgTypeClass !== null && spec.elemPgTypeClass !== undefined) {
    return false; // array types
  }
  if (spec.pgTypeClass === PostgresTypeClass.Enum) {
    return false; // enum sort order is by attnum, not by text
  }
  const {typeOID} = spec;
  if (TEXT_TYPES.has(typeOID)) {
    // A non-deterministic collation is excluded; no collation at all
    // (`null`/`undefined`) is not expected for a text column, but is treated
    // conservatively as unknown.
    return spec.collationIsDeterministic === true;
  }
  return INT_TYPES.has(typeOID) || typeOID === UUID || typeOID === BOOL;
}

/**
 * Whether every column of the row key is resumable. When this is false the
 * change source never attaches a `lastKey` to its `backfill` messages, so no
 * subscriber ever holds a mark for the table and every run starts from the
 * beginning.
 */
export function isResumableKey(specs: readonly ResumeColumnSpec[]): boolean {
  return specs.length > 0 && specs.every(isResumableColumn);
}

/**
 * Escapes a string as a Postgres `E'...'` literal, which has the same meaning
 * whether or not `standard_conforming_strings` is on.
 *
 * A plain `'...'` literal would suffice under the default (and, since PG 9.1,
 * only supported) setting, but the `E` form makes the escaping explicit and
 * independent of server configuration.
 */
function escapeString(value: string): string {
  return `E'${value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
}

/**
 * Renders one mark value as a SQL literal.
 *
 * The literal must be a *constant* expression so that Postgres can use it as
 * an index condition. (This rules out, e.g.,
 * `convert_from(decode(...), 'UTF8')`, which is `STABLE` and therefore
 * degrades a resume seek into a filtered full index scan.)
 *
 * @throws if `text` is not a valid value for the column's type, which
 *   indicates a corrupt or forged mark.
 */
export function keyLiteral(spec: ResumeColumnSpec, text: string): string {
  const {typeOID} = spec;
  if (INT_TYPES.has(typeOID)) {
    if (!INT_RE.test(text)) {
      throw new InvalidMarkError(`${text} is not an integer`);
    }
    return text;
  }
  if (typeOID === UUID) {
    if (!UUID_RE.test(text)) {
      throw new InvalidMarkError(`${text} is not a uuid`);
    }
    return `'${text.toLowerCase()}'::uuid`;
  }
  if (typeOID === BOOL) {
    if (text !== 'true' && text !== 'false') {
      throw new InvalidMarkError(`${text} is not a boolean`);
    }
    return text;
  }
  if (TEXT_TYPES.has(typeOID)) {
    return escapeString(text);
  }
  throw new InvalidMarkError(`type OID ${typeOID} is not resumable`);
}

export class InvalidMarkError extends Error {
  readonly name = 'InvalidMarkError';
}

/**
 * The `ORDER BY` clause for an ordered backfill: the row key columns in
 * `relation.rowKey.columns` order, each in its own (native) collation.
 *
 * Ordering in the column's native collation — rather than `COLLATE "C"` —
 * keeps the key's btree usable for both the ordering and the resume seek. Any
 * total, snapshot-stable order suffices, because keys are only ever compared
 * by Postgres.
 */
export function orderByRowKey(rowKeyColumns: readonly string[]): string {
  return rowKeyColumns.map(id).join(',');
}

/**
 * A boolean SQL expression selecting rows whose key sorts strictly after
 * `mark`, using a row-constructor comparison so that it is index-optimizable
 * on the key's btree.
 */
export function resumeWhere(
  rowKeyColumns: readonly string[],
  specs: readonly ResumeColumnSpec[],
  mark: Mark,
): string {
  return rowComparison(rowKeyColumns, specs, mark, '>');
}

function rowComparison(
  rowKeyColumns: readonly string[],
  specs: readonly ResumeColumnSpec[],
  mark: Mark,
  op: '>' | '<=',
): string {
  if (rowKeyColumns.length !== mark.length) {
    throw new InvalidMarkError(
      `mark has ${mark.length} values for a ${rowKeyColumns.length} column key`,
    );
  }
  const cols = rowKeyColumns.map(id).join(',');
  const vals = mark.map((text, i) => keyLiteral(specs[i], text)).join(',');
  return `(${cols}) ${op} (${vals})`;
}

/**
 * The row filter of the table's publications, as a parenthesized boolean
 * expression, or `null` if the table is published without a filter.
 */
export function publicationRowFilter(table: PublishedTableSpec): string | null {
  const filters = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f);
  return filters.length === 0 ? null : `(${filters.join(' OR ')})`;
}

/**
 * Returns whether any row of `table` has a key in `(from, to]`, under the
 * table's publication row filter. `from === null` means "from the beginning",
 * in which case the answer is whether any row exists up to and including `to`.
 *
 * This is the only key comparison the system performs outside of the backfill
 * COPY itself, and it is what the {@link BackfillManager} asks in order to
 * decide whether a declaring subscriber needs rows that the running run has
 * already passed.
 */
export async function rowsExist(
  sql: PostgresDB | PostgresTransaction,
  table: PublishedTableSpec,
  rowKeyColumns: readonly string[],
  specs: readonly ResumeColumnSpec[],
  from: Mark | null,
  to: Mark,
): Promise<boolean> {
  const conditions = [
    publicationRowFilter(table),
    from === null ? null : rowComparison(rowKeyColumns, specs, from, '>'),
    rowComparison(rowKeyColumns, specs, to, '<='),
  ].filter(c => c !== null);
  const rows = await sql.unsafe(
    /*sql*/ `SELECT 1 FROM ${id(table.schema)}.${id(table.name)} ` +
      `WHERE ${conditions.join(' AND ')} LIMIT 1`,
  );
  return rows.length > 0;
}

/**
 * Converts a decoded row key value to its mark (Postgres text) form.
 *
 * Accepts the shapes produced by both the binary COPY decoders
 * (`pg-copy-binary.ts`) and the text COPY type parsers.
 */
export function textKey(spec: ResumeColumnSpec, value: unknown): string {
  const {typeOID} = spec;
  if (value === null || value === undefined) {
    // Row key columns are `NOT NULL` (see `db/lite-tables.ts`), so this
    // indicates a bug rather than data.
    throw new InvalidMarkError(`null value in a row key`);
  }
  if (typeOID === BOOL) {
    switch (typeof value) {
      case 'boolean':
        return value ? 'true' : 'false';
      case 'number':
      case 'bigint':
        return value ? 'true' : 'false';
      case 'string':
        return value === 't' || value === 'true' ? 'true' : 'false';
    }
  }
  const text = typeof value === 'string' ? value : String(value);
  if (typeOID === UUID) {
    return text.toLowerCase();
  }
  return text;
}

/**
 * Computes the mark of the last row of a batch of `rowValues`, whose leading
 * values are the row key values (see the `backfill` message).
 */
export function markOfLastRow(
  specs: readonly ResumeColumnSpec[],
  rowValues: readonly (readonly unknown[])[],
): string[] {
  const last = must(rowValues.at(-1), `no rows`);
  return specs.map((spec, i) => textKey(spec, last[i]));
}

/**
 * Reads the collation determinism of the specified columns.
 *
 * `PublishedTableSpec` does not carry this (the published schema strips it
 * during canonicalization), so it is read directly. `null` means the column
 * has no collation, e.g. an integer or uuid column.
 */
export async function getKeyCollations(
  sql: PostgresDB | PostgresTransaction,
  relationOID: number,
  columns: readonly string[],
): Promise<Map<string, boolean | null>> {
  const rows = await sql<{col: string; deterministic: boolean | null}[]>`
    SELECT attname AS "col", coll.collisdeterministic AS "deterministic"
      FROM pg_attribute
      LEFT JOIN pg_collation coll ON coll.oid = attcollation
      WHERE attrelid = ${relationOID} AND attname IN ${sql(columns as string[])}`;
  return new Map(rows.map(({col, deterministic}) => [col, deterministic]));
}

/**
 * The minimum `pg_stats.correlation` of a row key's leading column for its
 * table's backfill to be ordered (and therefore resumable).
 *
 * Ordering a backfill COPY by the row key means walking the key's index in
 * order and fetching each row from the heap. When the heap's physical order
 * matches the key order the fetches are sequential and ordering is nearly
 * free; when it does not, each of N rows is a scattered visit into the same
 * pages, and throughput collapses.
 *
 * Measured on 1M rows (~155MB, warm cache, PG 17) with
 * `backfill-resume.bench.pg.ts`, ordered COPY as a fraction of unordered:
 *
 * | correlation | ordered / unordered |
 * | ----------- | ------------------- |
 * | 1.0         | 82%                 |
 * | 0.99995     | 51%                 |
 * | 0.995       | 32%                 |
 * | 0.70        | 22%                 |
 * | 0.0 (uuid)  | 18%                 |
 * | 0.0 (text)  | 17%                 |
 *
 * Correlation saturates near 1, so the threshold has to be tight to hold
 * ordering within ~2x of an unordered COPY. In practice this admits
 * append-mostly tables with a monotonic key, and `CLUSTER`ed tables; a table
 * with a random key (uuid, nanoid) is not ordered, and so is not resumable,
 * and keeps exactly today's behavior.
 */
export const MIN_KEY_CORRELATION = 0.9999;

/**
 * Reads `pg_stats.correlation` for the leading column of the row key: the
 * statistical correlation between the column's value order and the physical
 * row order.
 *
 * Returns `null` if the table has never been analyzed, or if the column has
 * no correlation statistic. Both are treated as "do not order" by
 * {@link isCheaplyOrderable}, which keeps today's behavior for a table whose
 * cost is unknown.
 */
export async function getKeyCorrelation(
  sql: PostgresDB | PostgresTransaction,
  relationOID: number,
  leadingKeyColumn: string,
): Promise<number | null> {
  const rows = await sql<{correlation: number | null}[]>`
    SELECT s.correlation
      FROM pg_stats s, pg_class c, pg_namespace n
     WHERE c.oid = ${relationOID}
       AND n.oid = c.relnamespace
       AND s.schemaname = n.nspname
       AND s.tablename = c.relname
       AND s.attname = ${leadingKeyColumn}`;
  return rows[0]?.correlation ?? null;
}

/**
 * Whether ordering a backfill of the table by its row key is cheap enough to
 * be worth doing, based on {@link getKeyCorrelation}.
 */
export function isCheaplyOrderable(
  correlation: number | null,
  minCorrelation: number = MIN_KEY_CORRELATION,
): boolean {
  return correlation !== null && Math.abs(correlation) >= minCorrelation;
}
