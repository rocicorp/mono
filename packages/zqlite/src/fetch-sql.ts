import {assignProperty} from '../../shared/src/objects.ts';
import type {Ordering} from '../../zero-protocol/src/ast.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import type {Constraint} from '../../zql/src/ivm/constraint.ts';
import type {
  FetchRequest,
  MultiConstraint,
} from '../../zql/src/ivm/operator.ts';
import {format} from './internal/sql.ts';
import {
  Binding,
  buildSelectQueries,
  toSQLiteType,
  type NoSubqueryCondition,
} from './query-builder.ts';

export type FetchSQL = {
  readonly text: string;
  readonly values: readonly unknown[];
};

/**
 * More shapes than a connection is expected to fetch. Past it the cache is
 * cleared, which bounds it if some fetch parameter that is part of the shape
 * (a `req.filter` that is not the same object every time, say) never repeats.
 */
const MAX_SHAPES = 256;

const filterIDs = new WeakMap<NoSubqueryCondition, number>();
let nextFilterID = 0;

/**
 * Builds the SELECTs of {@link buildSelectQueries} for the fetches of one
 * connection, formatting the SQL text only once per fetch shape.
 *
 * Building and formatting the SQL is a fixed cost of every fetch that is
 * comparable to running the statement for the small fetches IVM does most.
 * The text depends only on the fetch's shape, though: which columns it
 * constrains, how many entries each multi-constraint has, its direction,
 * which columns of its start row are NULL, and its `req.filter`. The values
 * are bound to `?` placeholders, so a shape's text is built once with a
 * {@link Binding} in place of each value, and each fetch only reads its values
 * from those.
 */
export class FetchSQLCache {
  readonly #tableName: string;
  readonly #columns: Record<string, SchemaValue>;
  readonly #filters: NoSubqueryCondition | undefined;
  readonly #order: Ordering | undefined;
  readonly #templates = new Map<string, readonly FetchSQL[]>();

  constructor(
    tableName: string,
    columns: Record<string, SchemaValue>,
    filters: NoSubqueryCondition | undefined,
    order: Ordering | undefined,
  ) {
    this.#tableName = tableName;
    this.#columns = columns;
    this.#filters = filters;
    this.#order = order;
  }

  get(req: FetchRequest): FetchSQL[] {
    const key = this.#shapeKey(req);
    let templates = this.#templates.get(key);
    if (templates === undefined) {
      templates = this.#buildTemplates(req);
      if (this.#templates.size >= MAX_SHAPES) {
        this.#templates.clear();
      }
      this.#templates.set(key, templates);
    }
    return templates.map(({text, values}) => ({
      text,
      values: values.map(v => (v instanceof Binding ? v.get(req) : v)),
    }));
  }

  // Column names are never empty and never contain NUL, so terminating each
  // with NUL and each list with an extra NUL keeps distinct shapes distinct.
  #shapeKey(req: FetchRequest): string {
    const {constraint, multiConstraints, start, filter} = req;
    let key = req.reverse ? 'r' : 'f';
    if (constraint) {
      for (const k in constraint) {
        key += k + '\0';
      }
    }
    key += '\0';
    if (multiConstraints) {
      for (const mc of multiConstraints) {
        key += mc.length + '\0';
        if (mc.length > 0) {
          for (const k in mc[0]) {
            key += k + '\0';
          }
        }
        key += '\0';
      }
    }
    key += '\0';
    if (start) {
      key += start.basis;
      // Only the order's columns of the start row are read.
      for (const [field] of this.#order ?? []) {
        key += isNullish(start.row[field]) ? '0' : '1';
      }
    }
    key += '\0';
    if (filter) {
      let id = filterIDs.get(filter);
      if (id === undefined) {
        id = nextFilterID++;
        filterIDs.set(filter, id);
      }
      key += id;
    }
    return key;
  }

  #buildTemplates(req: FetchRequest): FetchSQL[] {
    const columns = this.#columns;
    const binding = (
      get: (req: FetchRequest) => unknown,
      field: string,
    ): Binding => {
      const {type} = columns[field];
      return new Binding(req => toSQLiteType(get(req), type));
    };

    let constraint: Constraint | undefined;
    if (req.constraint) {
      const probe: Record<string, unknown> = {};
      for (const k in req.constraint) {
        assignProperty(
          probe,
          k,
          binding(req => req.constraint![k], k),
        );
      }
      constraint = probe as Constraint;
    }
    const multiConstraints = req.multiConstraints?.map(
      (mc, i): MultiConstraint =>
        mc.map((entry, j) => {
          const probe: Record<string, unknown> = {};
          for (const k in entry) {
            assignProperty(
              probe,
              k,
              binding(req => req.multiConstraints![i][j][k], k),
            );
          }
          return probe as Row;
        }),
    );
    let start: FetchRequest['start'];
    if (req.start) {
      const row: Record<string, unknown> = {};
      for (const [field] of this.#order ?? []) {
        assignProperty(
          row,
          field,
          isNullish(req.start.row[field])
            ? null
            : binding(req => req.start!.row[field], field),
        );
      }
      start = {basis: req.start.basis, row: row as Row};
    }

    return buildSelectQueries(
      this.#tableName,
      columns,
      constraint,
      this.#filters,
      this.#order,
      req.reverse,
      start,
      multiConstraints,
      req.filter,
    ).map(query => format(query));
  }
}

function isNullish(v: unknown): boolean {
  return v === null || v === undefined;
}
