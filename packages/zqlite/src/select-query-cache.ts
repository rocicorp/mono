import type {Ordering} from '../../zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import {
  compileSelectQuery,
  type CompiledSelectQuery,
  type NoSubqueryCondition,
  type SelectRequest,
} from './query-builder.ts';

/**
 * The default maximum number of request shapes compiled by a
 * {@link SelectQueryCache}. A connection's fetches usually come in a handful
 * of shapes; the bound guards against the ones that multiply, such as the
 * lengths of a multi-constraint's `IN` list.
 */
export const DEFAULT_MAX_SELECT_QUERY_SHAPES = 256;

let nextFilterID = 0;
const filterIDs = new WeakMap<NoSubqueryCondition, string>();

function filterID(filter: NoSubqueryCondition): string {
  let id = filterIDs.get(filter);
  if (id === undefined) {
    id = String(nextFilterID++);
    filterIDs.set(filter, id);
  }
  return id;
}

/**
 * The select queries of one connection of a table source, compiled once for
 * each shape of request (see {@link compileSelectQuery}), so that building a
 * fetch's SQL, and normalizing it for the statement cache, is not repeated
 * for every fetch.
 *
 * The connection's filters and ordering are fixed. A request's `filter` is
 * identified by the object: `FilterStart` passes the same object for the same
 * condition.
 */
export class SelectQueryCache {
  readonly #table: string;
  readonly #columns: Record<string, SchemaValue>;
  readonly #filters: NoSubqueryCondition | undefined;
  readonly #order: Ordering | undefined;
  readonly #maxSize: number;
  readonly #compiled = new Map<string, CompiledSelectQuery>();

  constructor(
    table: string,
    columns: Record<string, SchemaValue>,
    filters: NoSubqueryCondition | undefined,
    order: Ordering | undefined,
    maxSize = DEFAULT_MAX_SELECT_QUERY_SHAPES,
  ) {
    this.#table = table;
    this.#columns = columns;
    this.#filters = filters;
    this.#order = order;
    this.#maxSize = maxSize;
  }

  get size(): number {
    return this.#compiled.size;
  }

  /** Returns the SQL for `req` and the values of its parameters. */
  get(req: SelectRequest): {text: string; values: unknown[]} {
    const key = this.#shapeKey(req);
    let compiled = this.#compiled.get(key);
    if (compiled === undefined) {
      compiled = compileSelectQuery(
        this.#table,
        this.#columns,
        req,
        this.#filters,
        this.#order,
      );
      if (this.#compiled.size >= this.#maxSize) {
        this.#compiled.clear();
      }
      this.#compiled.set(key, compiled);
    }
    const {text, bindings} = compiled;
    const values = new Array<unknown>(bindings.length);
    for (let i = 0; i < bindings.length; i++) {
      values[i] = bindings[i](req);
    }
    return {text, values};
  }

  /**
   * Identifies everything about `req` that the SQL depends on, i.e. all of it
   * except the values that are bound to its parameters.
   */
  #shapeKey({
    constraint,
    multiConstraints,
    start,
    reverse,
    filter,
  }: SelectRequest): string {
    let key = reverse ? 'r' : 'f';
    key += filter === undefined ? '' : filterID(filter);
    key += '\x01';
    if (constraint !== undefined) {
      for (const k of Object.keys(constraint)) {
        key += k + '\x00';
      }
    }
    key += '\x01';
    if (multiConstraints !== undefined) {
      for (const mc of multiConstraints) {
        key += mc.length;
        if (mc.length > 0) {
          for (const k of Object.keys(mc[0])) {
            key += '\x00' + k;
          }
        }
        key += '\x02';
      }
    }
    key += '\x01';
    if (start !== undefined) {
      key += start.basis === 'at' ? 'a' : 'b';
      // A NULL start value is written into the SQL rather than bound (see
      // gatherStartConstraints()). JSON values are bound as their JSON text,
      // which is never NULL.
      for (const [field] of this.#order ?? []) {
        key +=
          this.#columns[field].type !== 'json' &&
          (start.row[field] ?? null) === null
            ? '0'
            : '1';
      }
    }
    return key;
  }
}
