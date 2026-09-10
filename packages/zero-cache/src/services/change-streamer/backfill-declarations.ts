import {unreachable} from '../../../../shared/src/asserts.ts';
import type {
  BackfillID,
  Identifier,
  TableMetadata,
} from '../change-source/protocol/current/data.ts';
import {isSchemaChange} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {schemaChangeTags} from '../change-source/protocol/current/schema-change-tags.ts';
import type {BackfillRequestMessage} from '../change-source/protocol/current/upstream.ts';
import {cookieOps, type CookieOp} from '../replicator/change-log-cookies.ts';
import type {BackfillDeclaration} from './change-streamer.ts';

type Column = {
  backfill: BackfillID;
  runID: string | null;
  runSeq: number | null;
  covered: boolean;
};

type Table = {
  table: Identifier & {metadata: TableMetadata | null};
  columns: Map<string, Column>;
};

const key = ({schema, name}: Identifier) => JSON.stringify([schema, name]);

// The data messages that can move a tracked column's obligation or run.
// Transaction boundaries are not among them: they are applied without being
// parsed (see `begin()`). Nor are row changes: no mark is tracked here, so a
// row key change moves nothing.
const TRACKED_TAGS = new Set<string>([
  ...schemaChangeTags,
  'backfill',
  'backfill-started',
]);

// With nothing tracked, the only messages that can change anything are those
// that start a backfill, so an idle subscriber parses nothing else.
const IDLE_TRACKED_TAGS = new Set<string>(['create-table', 'add-column']);

/**
 * Follows declarations through the same ordered, batched stream delivered to
 * the subscriber. This works with either catchup store and with the live
 * backlog, without a separate log read or retention pin. Only a followed
 * completion or a schema drop discharges a column's backfill obligation.
 *
 * The one question tracked is whether the subscriber is following the run its
 * table has, decided as the subscriber's replica decides it
 * (`processBackfillStarted`). A column that is not following any run is
 * requested of the change source, which answers with a run from the
 * beginning that every subscriber follows.
 *
 * A backfill that the stream starts is an obligation like a declared one, so a
 * subscriber that declares nothing is tracked all the same: its replica follows
 * runs regardless, and nothing else would request a run that it cannot follow.
 *
 * State is committed with the stream transaction. In particular, a rolled
 * back announcement cannot cover a subscriber or suppress its next request.
 */
export class BackfillDeclarations {
  #tables = new Map<string, Table>();
  // The open transaction's copy of #tables, made at its first change to them,
  // so that a transaction that changes nothing copies nothing.
  #transaction: Map<string, Table> | undefined;
  #inTransaction = false;

  constructor(declarations: readonly BackfillDeclaration[]) {
    for (const declaration of declarations) {
      const table = {
        schema: declaration.schema,
        name: declaration.table,
        metadata: declaration.metadata ?? null,
      };
      this.#tables.set(key(table), {
        table,
        columns: new Map(
          declaration.columns.map(column => [
            column,
            {
              backfill: declaration.backfill?.[column] ?? {},
              runID: declaration.runID,
              runSeq: declaration.runSeq,
              covered: false,
            },
          ]),
        ),
      });
    }
  }

  get inTransaction(): boolean {
    return this.#inTransaction;
  }

  get pending(): boolean {
    return this.#current.size > 0;
  }

  /** Whether {@link apply} needs to see a data message with this tag. */
  tracks(tag: string): boolean {
    return (this.pending ? TRACKED_TAGS : IDLE_TRACKED_TAGS).has(tag);
  }

  begin(): void {
    this.#inTransaction = true;
    this.#transaction = undefined;
  }

  commit(): void {
    this.#tables = this.#transaction ?? this.#tables;
    this.#transaction = undefined;
    this.#inTransaction = false;
  }

  rollback(): void {
    this.#transaction = undefined;
    this.#inTransaction = false;
  }

  apply([type, change]: ChangeStreamData): void {
    if (type === 'begin') {
      this.begin();
      return;
    }
    if (type === 'commit') {
      this.commit();
      return;
    }
    if (type === 'rollback') {
      this.rollback();
      return;
    }
    if (!this.#inTransaction) {
      return;
    }

    switch (change.tag) {
      case 'backfill-started':
      case 'backfill':
      case 'backfill-completed': {
        const table = this.#writableTable(change.relation);
        if (!table) {
          return;
        }
        for (const name of [
          ...change.relation.rowKey.columns,
          ...change.columns,
        ]) {
          const column = table.columns.get(name);
          if (!column) {
            continue;
          }
          if (change.tag === 'backfill-started') {
            // The replicator's rule (`processBackfillStarted`).
            const {runID, resumes} = change;
            column.covered =
              resumes === null ||
              column.runID === runID ||
              (column.runID === resumes.runID &&
                column.runSeq !== null &&
                column.runSeq >= resumes.seq);
            if (!column.covered) {
              column.runID = null;
              column.runSeq = null;
            } else if (column.runID !== runID) {
              column.runID = runID;
              column.runSeq = 0;
            }
          } else if (change.tag === 'backfill') {
            // Rows of the run the subscriber declared: it was following the
            // run before it subscribed, and the run's announcement predates
            // the subscription -- unless the batch's position skips ahead of
            // the subscriber's, in which case it missed a batch, and the
            // replicator's rule (`processBackfill`) takes it off the run.
            if (change.runID !== undefined && column.runID === change.runID) {
              const {seq} = change;
              if (
                seq !== undefined &&
                column.runSeq !== null &&
                seq > column.runSeq + 1
              ) {
                column.runID = null;
                column.runSeq = null;
                column.covered = false;
              } else {
                column.covered = true;
                column.runSeq =
                  seq === undefined
                    ? column.runSeq
                    : Math.max(column.runSeq ?? 0, seq);
              }
            }
          } else if (
            change.runID === undefined ||
            (column.runID === change.runID &&
              (change.seq === undefined || column.runSeq === change.seq))
          ) {
            table.columns.delete(name);
          } else {
            // This manager finished a run the subscriber did not follow, or
            // did not follow to its end.
            column.covered = false;
          }
        }
        if (table.columns.size === 0) {
          this.#writable().delete(key(table.table));
        }
        return;
      }
    }

    if (!isSchemaChange(change)) {
      return;
    }
    const ops = cookieOps(change);
    for (const op of ops) {
      const id = op.op === 'rename-table' ? op.old : op.table;
      let table = this.#writableTable(id);
      if (!table) {
        if (op.op !== 'upsert-backfill') {
          continue;
        }
        // A backfill that the stream started, on a table with nothing else in
        // flight. The subscriber's replica applies this same change.
        table = {
          table: {
            schema: id.schema,
            name: id.name,
            metadata: metadataIn(ops, id),
          },
          columns: new Map(),
        };
        this.#writable().set(key(id), table);
      }
      const tables = this.#writable();
      switch (op.op) {
        case 'upsert-metadata':
          table.table.metadata = op.metadata;
          break;
        case 'upsert-backfill':
          // Covered until a run's announcement says otherwise. The source that
          // sent this change holds the obligation, and the run it starts for
          // it is announced to every subscriber. A request before then would
          // only queue a run that the subscriber is about to follow (see
          // `BackfillManager.onBackfillRequest`), and a new source session is
          // told about covered columns regardless (see `requests()`).
          table.columns.set(op.column, {
            backfill: op.backfill,
            runID: null,
            runSeq: null,
            covered: true,
          });
          break;
        case 'rename-table':
          tables.delete(key(op.old));
          table.table = {...op.new, metadata: table.table.metadata};
          tables.set(key(op.new), table);
          break;
        case 'drop-table':
          tables.delete(key(op.table));
          break;
        case 'rename-column': {
          const column = table.columns.get(op.old);
          if (column) {
            table.columns.delete(op.old);
            table.columns.set(op.new, column);
          }
          break;
        }
        case 'drop-column':
          table.columns.delete(op.column);
          if (table.columns.size === 0) {
            tables.delete(key(op.table));
          }
          break;
        case 'complete-backfill':
          // Handled above with the subscriber's run-following rule.
          break;
        case 'invalidate-marks':
          break; // Only produced by markOps, not cookieOps.
        default:
          unreachable(op);
      }
    }
  }

  /**
   * A new change-source session must hear about every unfinished column, even
   * one covered by the previous session. Otherwise a source restart can lose
   * a replacement run requested only by this subscriber.
   */
  requests(
    subscriberID: string,
    includeCovered = false,
  ): BackfillRequestMessage[] {
    const requests: BackfillRequestMessage[] = [];
    for (const {table, columns} of this.#tables.values()) {
      const states = [...columns.values()];
      if (!states.length || (!includeCovered && states.every(c => c.covered))) {
        continue;
      }
      const [first] = states;
      const oneRun = states.every(
        c => c.runID === first.runID && c.runSeq === first.runSeq,
      );
      requests.push([
        'backfill-request',
        {
          table,
          columns: Object.fromEntries(
            Array.from(columns, ([name, c]) => [name, c.backfill]),
          ),
          runID: oneRun ? first.runID : null,
          runSeq: oneRun ? first.runSeq : null,
          subscriberID,
        },
      ]);
    }
    return requests;
  }

  get #current(): Map<string, Table> {
    return this.#transaction ?? this.#tables;
  }

  #writable(): Map<string, Table> {
    return (this.#transaction ??= structuredClone(this.#tables));
  }

  /** The table's entry in the transaction's copy, if the table is tracked. */
  #writableTable(id: Identifier): Table | undefined {
    return this.#current.has(key(id))
      ? this.#writable().get(key(id))
      : undefined;
  }
}

/**
 * The metadata a schema change upserts for the table, which is the only source
 * of it for a table that nothing declared.
 */
function metadataIn(
  ops: readonly CookieOp[],
  {schema, name}: Identifier,
): TableMetadata | null {
  for (const op of ops) {
    if (
      op.op === 'upsert-metadata' &&
      op.table.schema === schema &&
      op.table.name === name
    ) {
      return op.metadata;
    }
  }
  return null;
}
