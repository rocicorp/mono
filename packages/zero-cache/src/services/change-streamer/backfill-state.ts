import {unreachable} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {deepEqual} from '../../../../shared/src/json.ts';
import {
  acceptBackfill,
  covers,
} from '../change-source/protocol/backfill-progress.ts';
import type {
  BackfillRequest,
  ChangeStreamData,
  Identifier,
  SchemaChange,
  TableMetadata,
} from '../change-source/protocol/current.ts';
import {schemaChangeTags} from '../change-source/protocol/current/schema-change-tags.ts';
import {cookieOps} from '../replicator/change-log-cookies.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';

function tableKey({schema, name}: Identifier) {
  return JSON.stringify([schema, name]);
}

type Column = BackfillRequest['columns'][string];

/** A mutable BackfillRequest. */
type Backfill = {
  table: {schema: string; name: string; metadata: TableMetadata | null};
  columns: Record<string, Column>;
};

type State = Map<string, Backfill>;

function clone(state: State): State {
  return new Map(
    Array.from(state, ([key, {table, columns}]) => [
      key,
      {table: {...table}, columns: {...columns}},
    ]),
  );
}

/**
 * Tags of messages that affect the backfill state. Other messages need not be
 * parsed.
 */
const RELEVANT_TAGS = new Set<ChangeTag>([
  'backfill',
  ...schemaChangeTags.filter(t => t !== 'create-index' && t !== 'drop-index'),
]);

// Serialized changes are typically forwarded (as the same string) to every
// subscriber, so a single entry cache avoids re-parsing the same message for
// each subscriber.
let lastParsed: {json: string; change: ChangeStreamData} | undefined;

function parse(json: string): ChangeStreamData {
  if (lastParsed?.json !== json) {
    lastParsed = {json, change: BigIntJSON.parse(json) as ChangeStreamData};
  }
  return lastParsed.change;
}

/**
 * Tracks the state of the pending backfills (and their progress) of a party
 * in the replication stream (e.g. a subscriber, or the change-log itself), by
 * applying the same policies that a subscriber applies to its replica:
 * schema changes are folded with the {@link cookieOps} shared by the
 * cookie jars, and `backfill` / `backfill-completed` messages are accepted
 * or ignored with the continuity policy of {@link acceptBackfill}.
 *
 * Changes are applied transactionally: the changes of a transaction are
 * discarded if it is rolled back, and {@link requests()} only reflects
 * committed transactions.
 */
export class BackfillState {
  #state: State;
  #beforeTx: State | null = null;

  constructor(requests: readonly BackfillRequest[] = []) {
    this.#state = new Map(
      requests
        .filter(req => Object.keys(req.columns).length > 0)
        .map(({table, columns}) => [
          tableKey(table),
          {table: {...table}, columns: {...columns}},
        ]),
    );
  }

  get #committed(): State {
    return this.#beforeTx ?? this.#state;
  }

  /** The (committed) pending backfills. */
  requests(): BackfillRequest[] {
    return [...clone(this.#committed).values()];
  }

  /**
   * Returns the `requests` with the (committed) progress of this state
   * applied to the columns that are pending in both, i.e. same table, column,
   * and backfill ID. Other columns have no progress (i.e. from scratch).
   */
  withProgress(requests: readonly BackfillRequest[]): BackfillRequest[] {
    const state = this.#committed;
    return requests.map(({table, columns}) => {
      const tracked = state.get(tableKey(table))?.columns;
      return {
        table,
        columns: Object.fromEntries(
          Object.entries(columns).map(([name, {id}]) => {
            const col = tracked?.[name];
            return [
              name,
              col?.progress && deepEqual(col.id, id)
                ? {id, progress: col.progress}
                : {id},
            ];
          }),
        ),
      };
    });
  }

  /** Whether there are no (committed) pending backfills. */
  get empty(): boolean {
    return this.#committed.size === 0;
  }

  /**
   * Returns the columns (as `schema.table.column` strings) with (committed)
   * pending backfills that are not covered by the (committed) progress of the
   * `stream`, i.e. columns for which the `stream` will not deliver all of the
   * necessary data.
   */
  uncovered(stream: BackfillState): string[] {
    const uncovered: string[] = [];
    const streamState = stream.#committed;
    for (const [key, {table, columns}] of this.#committed) {
      const streamColumns = streamState.get(key)?.columns;
      for (const [name, {id, progress}] of Object.entries(columns)) {
        const s = streamColumns?.[name];
        if (!s || !deepEqual(s.id, id) || !covers(s.progress, progress)) {
          uncovered.push(`${table.schema}.${table.name}.${name}`);
        }
      }
    }
    return uncovered;
  }

  /**
   * Applies a serialized change, parsing it only if it is relevant.
   *
   * @returns The columns (as `schema.table.column` strings) of a backfill
   *          message that were ignored due to the continuity policy.
   */
  applySerialized([, tag, json]: WatermarkedChange): string[] {
    switch (tag) {
      case 'begin':
        this.#begin();
        return [];
      case 'commit':
        this.#commit();
        return [];
      case 'rollback':
        this.#rollback();
        return [];
    }
    if (!RELEVANT_TAGS.has(tag)) {
      return [];
    }
    return this.apply(parse(json));
  }

  /**
   * Applies a change.
   *
   * @returns The columns (as `schema.table.column` strings) of a backfill
   *          message that were ignored due to the continuity policy.
   */
  apply(change: ChangeStreamData): string[] {
    switch (change[0]) {
      case 'begin':
        this.#begin();
        return [];
      case 'commit':
        this.#commit();
        return [];
      case 'rollback':
        this.#rollback();
        return [];
    }
    const msg = change[1];
    switch (msg.tag) {
      case 'backfill': {
        const {relation, columns, progressMarks} = msg;
        return this.#applyBackfill(relation, columns, (progress, col) => {
          const result = acceptBackfill(progress, progressMarks);
          if (result.accept) {
            col.progress = result.progress;
            if (col.progress === undefined) {
              delete col.progress;
            }
          }
          return result.accept;
        });
      }
      case 'backfill-completed': {
        const {relation, columns, progressMarks} = msg;
        const backfill = this.#state.get(tableKey(relation));
        const ignored = this.#applyBackfill(
          relation,
          columns,
          (progress, _, name) => {
            const {accept} = acceptBackfill(
              progress,
              progressMarks && {previous: progressMarks.previous},
            );
            if (accept && backfill) {
              delete backfill.columns[name];
            }
            return accept;
          },
        );
        if (backfill && Object.keys(backfill.columns).length === 0) {
          this.#state.delete(tableKey(relation));
        }
        return ignored;
      }
      case 'create-table':
      case 'rename-table':
      case 'update-table-metadata':
      case 'add-column':
      case 'update-column':
      case 'drop-column':
      case 'drop-table':
        this.#applySchemaChange(msg);
        return [];
    }
    return [];
  }

  #applyBackfill(
    relation: {schema: string; name: string; rowKey: {columns: string[]}},
    columns: readonly string[],
    apply: (progress: Column['progress'], col: Column, name: string) => boolean,
  ): string[] {
    const backfill = this.#state.get(tableKey(relation));
    if (!backfill) {
      return [];
    }
    const ignored: string[] = [];
    // rowKey columns are excluded from `columns` but are backfilled with them
    // (e.g. for a new table).
    for (const name of [...relation.rowKey.columns, ...columns]) {
      const col = backfill.columns[name];
      if (!col) {
        continue;
      }
      // Copy-on-write, since the column object may be shared with the
      // pre-transaction state.
      const updated = {...col};
      if (apply(col.progress, updated, name)) {
        if (name in backfill.columns) {
          backfill.columns[name] = updated;
        }
      } else {
        ignored.push(`${relation.schema}.${relation.name}.${name}`);
      }
    }
    return ignored;
  }

  #applySchemaChange(change: SchemaChange) {
    // Table metadata specified in the same change (e.g. a `create-table` or
    // `add-column`), for backfills of tables that are not yet tracked.
    const metadata = new Map<string, TableMetadata>();

    for (const op of cookieOps(change)) {
      switch (op.op) {
        case 'upsert-metadata': {
          const key = tableKey(op.table);
          metadata.set(key, op.metadata);
          const backfill = this.#state.get(key);
          if (backfill) {
            backfill.table.metadata = op.metadata;
          }
          break;
        }
        case 'upsert-backfill': {
          const key = tableKey(op.table);
          let backfill = this.#state.get(key);
          if (!backfill) {
            backfill = {
              table: {
                schema: op.table.schema,
                name: op.table.name,
                metadata: metadata.get(key) ?? null,
              },
              columns: {},
            };
            this.#state.set(key, backfill);
          }
          // A new backfill always starts from scratch.
          backfill.columns[op.column] = {id: op.backfill};
          break;
        }
        case 'rename-table': {
          const backfill = this.#state.get(tableKey(op.old));
          if (backfill) {
            this.#state.delete(tableKey(op.old));
            backfill.table.schema = op.new.schema;
            backfill.table.name = op.new.name;
            this.#state.set(tableKey(op.new), backfill);
          }
          break;
        }
        case 'drop-table':
          this.#state.delete(tableKey(op.table));
          break;
        case 'rename-column': {
          const backfill = this.#state.get(tableKey(op.table));
          const col = backfill?.columns[op.old];
          if (backfill && col) {
            delete backfill.columns[op.old];
            backfill.columns[op.new] = col;
          }
          break;
        }
        case 'drop-column': {
          const key = tableKey(op.table);
          const backfill = this.#state.get(key);
          if (backfill) {
            delete backfill.columns[op.column];
            if (Object.keys(backfill.columns).length === 0) {
              this.#state.delete(key);
            }
          }
          break;
        }
        case 'complete-backfill':
          // Only produced for `backfill-completed`, which is handled
          // separately (with the continuity policy).
          throw new Error(`unexpected ${op.op} op`);
        default:
          unreachable(op);
      }
    }
  }

  #begin() {
    // Tolerate a missing commit/rollback, e.g. from an interrupted stream.
    this.#beforeTx ??= clone(this.#state);
  }

  #commit() {
    this.#beforeTx = null;
  }

  #rollback() {
    if (this.#beforeTx) {
      this.#state = this.#beforeTx;
      this.#beforeTx = null;
    }
  }
}
