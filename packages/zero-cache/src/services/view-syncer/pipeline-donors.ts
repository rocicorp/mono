import type {LogContext} from '@rocicorp/logger';
import {h64} from '../../../../shared/src/hash.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import {
  normalizeClientSchema,
  type ClientSchema,
} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {JoinIndex} from '../../../../zql/src/ivm/join-index.ts';
import type {Storage} from '../../../../zql/src/ivm/operator.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';

/** Past this, comparing one pipeline would hold up the worker too long. */
const DEFAULT_MAX_STATE_ENTRIES = 1_000_000;

/** The longest a hydration waits for a donor that is about to be ready. */
const DEFAULT_MAX_WAIT_MS = 250;

/**
 * Whether a query outputs synthetic aggregate rows, whose tables are named after
 * its query ID. Matching on the serialized query can only err towards requiring
 * the same query ID, which is always correct.
 */
export function usesAggregates(query: AST): boolean {
  const json = JSON.stringify(query);
  return json.includes('"aggregate":') || json.includes('"groupBy":');
}

/**
 * Gives a pipeline the state of a donor built from the same plan. It can't yield:
 * the donor's state can change once the donor runs again.
 *
 * @returns The number of operator state entries copied.
 */
export function copyPipelineState(
  donor: PipelineStateSource,
  operators: readonly Storage[],
  joinIndexes: readonly JoinIndex[],
  copyStorage?: (source: object, target: Storage) => number | undefined,
): number {
  let entries = 0;
  for (let i = 0; i < operators.length; i++) {
    const copied = copyStorage?.(donor.operators[i], operators[i]);
    if (copied !== undefined) {
      entries += copied;
      continue;
    }
    // Read in full first: operators can share a storage connection, which can't
    // write in the middle of a scan.
    const state = [...donor.operators[i].scan()];
    // A scalar subquery's pipeline has already fetched to resolve its value, and
    // what it wrote then has to give way to the donor's.
    const existing = [...operators[i].scan()];
    for (const [key] of existing) {
      operators[i].del(key);
    }
    for (const [key, value] of state) {
      operators[i].set(key, value);
    }
    entries += state.length;
  }
  must(donor.copyable).joinIndexes.forEach((index, i) =>
    joinIndexes[i].copyFrom(index),
  );
  return entries;
}

/** Identifies a client schema, for telemetry on which pipelines are alike. */
export function clientSchemaKey(schema: ClientSchema): string {
  return h64(JSON.stringify(normalizeClientSchema(schema))).toString(36);
}

/**
 * Identifies how a query's rows are keyed, which is all that a client schema
 * changes about its pipeline: the columns come from the replica. Clients whose
 * schemas differ only in other tables can share the query's pipelines.
 *
 * @param query The query before its scalar subqueries were resolved, since
 *        the rows behind them are synced too.
 */
export function rowKeysKey(
  query: AST,
  primaryKeys: ReadonlyMap<string, PrimaryKey>,
): string {
  const tables = [...tablesOf(query, new Set())].toSorted();
  return h64(
    JSON.stringify(tables.map(t => [t, primaryKeys.get(t) ?? null])),
  ).toString(36);
}

function tablesOf(query: AST, tables: Set<string>): Set<string> {
  tables.add(query.table);
  for (const {subquery} of query.related ?? []) {
    tablesOf(subquery, tables);
  }
  if (query.where) {
    conditionTables(query.where, tables);
  }
  return tables;
}

function conditionTables(condition: Condition, tables: Set<string>) {
  switch (condition.type) {
    case 'and':
    case 'or':
      for (const c of condition.conditions) {
        conditionTables(c, tables);
      }
      break;
    case 'correlatedSubquery':
      tablesOf(condition.related.subquery, tables);
      break;
    case 'simple':
      break;
  }
}

export type PipelineStateSource = {
  readonly version: () => string;
  /** Whether the pipeline's state is in the middle of changing. */
  readonly busy: () => boolean;
  readonly rowSetSignature: () => bigint | undefined;
  /** In the order they were built, which identical plans share. */
  readonly operators: readonly Pick<Storage, 'scan'>[];
  /** What a copy would have to take, when the source can be copied. */
  readonly copyable?: CopyableState | undefined;
};

export type CopyableState = {
  readonly queryID: string;
  /** The query with its scalar subqueries resolved to their values. */
  readonly query: AST;
  readonly joinIndexes: readonly JoinIndex[];
  /** What hydrating the query took, which a copy of it saves. */
  readonly hydrationTimeMs: number;
  /**
   * The rows the pipeline has output, with how many times it output each.
   * 'pending' while they can't be given consistently with the pipeline but
   * soon can, and undefined if they can't be.
   */
  readonly rows: () => readonly OutputRow[] | 'pending' | undefined;
};

/**
 * A donor to copy from, or how long it is worth waiting for one that is about
 * to be ready.
 */
export type DonorSearch =
  | {
      readonly source: PipelineStateSource;
      readonly rows: readonly OutputRow[];
    }
  | {readonly waitMs: number}
  | undefined;

export type OutputRow = {
  readonly table: string;
  readonly rowKey: Row;
  readonly count: number;
  /** The contents of a synthetic aggregate row, which isn't in the replica. */
  readonly row?: Row | undefined;
};

export type DonorResult = {
  readonly outcome:
    | 'no-donor'
    | 'version-mismatch'
    | 'donor-busy'
    | 'copied'
    | 'match'
    | 'donor-has-extra-state'
    | 'rows-differ'
    | 'state-differ'
    | 'too-large';
  readonly stateEntries: number;
  readonly compareTimeMs: number;
  /** How long the hydration waited for a donor to be ready. */
  readonly waitedMs?: number | undefined;
  /** Entries only the donor holds, which advancing can leave behind. */
  readonly extraDonorEntries?: number | undefined;
  /** The first entry that differs, for 'state-differ'. */
  readonly difference?: StateDifference | undefined;
};

export type StateDifference = {
  readonly operator: number;
  readonly key: string;
  readonly donor: ReadonlyJSONValue | undefined;
  readonly target: ReadonlyJSONValue | undefined;
};

/**
 * The pipelines of a sync worker that other client groups could take a hydration
 * from, because hydrating a query another group already has live repeats work.
 *
 * A pipeline's identity is `(rowKeysKey, transformationHash)`, and it can
 * only donate at the version the new one hydrates at. With `copy`, a matching
 * donor's state and rows replace the hydration. Without it, each group hydrates
 * and the result is compared with what would have been copied, which measures
 * how often copying could happen and whether it would be sound.
 *
 * The comparison includes donors that reached the version by advancing rather
 * than hydrating. Advancing can leave entries behind for rows the pipeline has
 * touched, which a fresh hydration would not create. Those are reported apart
 * from real differences, because a copy takes after the donor, not a hydration.
 *
 * Only operator storage is compared, not join indexes.
 */
export class PipelineDonors {
  readonly #lc: LogContext;
  readonly #maxStateEntries: number;
  readonly #maxWaitMs: number;
  readonly #onResult: ((result: DonorResult) => void) | undefined;
  readonly #sources = new Map<string, Set<PipelineStateSource>>();
  readonly #comparisons = getOrCreateCounter(
    'sync',
    'hydration-clone',
    'Hydrations copied from, or compared with, another client group, by outcome',
  );
  readonly #matchedHydrationMs = getOrCreateCounter(
    'sync',
    'hydration-clone-matched-hydration-ms',
    'Time spent hydrating queries whose result matched a possible copy',
  );
  readonly #waitMs = getOrCreateCounter(
    'sync',
    'hydration-clone-wait-ms',
    'Time hydrations spent waiting for a donor, by whether they then copied',
  );

  readonly copy: boolean;

  constructor({
    lc,
    copy = false,
    maxStateEntries = DEFAULT_MAX_STATE_ENTRIES,
    maxWaitMs = DEFAULT_MAX_WAIT_MS,
    onResult,
  }: {
    lc: LogContext;
    copy?: boolean | undefined;
    maxStateEntries?: number | undefined;
    maxWaitMs?: number | undefined;
    onResult?: ((result: DonorResult) => void) | undefined;
  }) {
    this.#lc = lc.withContext('component', 'pipeline-donors');
    this.copy = copy;
    this.#maxStateEntries = maxStateEntries;
    this.#maxWaitMs = maxWaitMs;
    this.#onResult = onResult;
  }

  /** @returns A function that stops offering the source as a donor. */
  register(key: string, source: PipelineStateSource): () => void {
    let sources = this.#sources.get(key);
    if (!sources) {
      sources = new Set();
      this.#sources.set(key, sources);
    }
    sources.add(source);
    return () => {
      const current = this.#sources.get(key);
      current?.delete(source);
      if (current?.size === 0) {
        this.#sources.delete(key);
      }
    };
  }

  /**
   * @returns A source to copy a pipeline from: one that can be copied, at the
   * version, that `matches`, and not in the middle of changing. Failing that,
   * how long it is worth waiting for one that is in the middle of changing,
   * which is never more than half of what it saves.
   */
  findDonor(
    key: string,
    version: string,
    matches: (source: PipelineStateSource) => boolean,
  ): DonorSearch {
    let waitMs = 0;
    for (const source of this.#sources.get(key) ?? []) {
      const {copyable} = source;
      if (!copyable || source.version() !== version || !matches(source)) {
        continue;
      }
      const rows = source.busy() ? 'pending' : copyable.rows();
      if (rows === 'pending') {
        waitMs = Math.max(
          waitMs,
          Math.min(this.#maxWaitMs, copyable.hydrationTimeMs / 2),
        );
      } else if (rows) {
        return {source, rows};
      }
    }
    return waitMs > 0 ? {waitMs} : undefined;
  }

  /** Records a hydration that was replaced by a copy. */
  recordCopy(
    key: string,
    stateEntries: number,
    copyTimeMs: number,
    waitedMs: number,
  ): void {
    this.#comparisons.add(1, {outcome: 'copied'});
    if (waitedMs > 0) {
      this.#waitMs.add(waitedMs, {copied: true});
    }
    const result = {
      outcome: 'copied',
      stateEntries,
      compareTimeMs: copyTimeMs,
      waitedMs,
    } as const;
    this.#lc.debug?.('hydration copied', {key, ...result});
    this.#onResult?.(result);
  }

  compare(
    key: string,
    target: PipelineStateSource,
    hydrationTimeMs: number,
    waitedMs = 0,
  ): DonorResult {
    const start = performance.now();
    const compared = this.#compare(key, target);
    const {outcome} = compared;
    const result = {
      ...compared,
      compareTimeMs: performance.now() - start,
      ...(waitedMs > 0 && {waitedMs}),
    };
    this.#comparisons.add(1, {outcome});
    if (waitedMs > 0) {
      this.#waitMs.add(waitedMs, {copied: false});
    }
    if (outcome === 'match') {
      this.#matchedHydrationMs.add(hydrationTimeMs);
    }
    const log =
      outcome === 'rows-differ' || outcome === 'state-differ'
        ? this.#lc.warn
        : this.#lc.debug;
    log?.call(this.#lc, `hydration clone: ${outcome}`, {
      key,
      hydrationTimeMs,
      ...result,
    });
    this.#onResult?.(result);
    return result;
  }

  #compare(
    key: string,
    target: PipelineStateSource,
  ): Omit<DonorResult, 'compareTimeMs'> {
    const version = target.version();
    let outcome: DonorResult['outcome'] = 'no-donor';
    for (const donor of this.#sources.get(key) ?? []) {
      if (donor === target) {
        continue;
      }
      if (donor.version() !== version) {
        outcome = outcome === 'donor-busy' ? outcome : 'version-mismatch';
      } else if (donor.busy()) {
        outcome = 'donor-busy';
      } else {
        return this.#compareWith(donor, target);
      }
    }
    return {outcome, stateEntries: 0};
  }

  #compareWith(
    donor: PipelineStateSource,
    target: PipelineStateSource,
  ): Omit<DonorResult, 'compareTimeMs'> {
    if (donor.rowSetSignature() !== target.rowSetSignature()) {
      return {outcome: 'rows-differ', stateEntries: 0};
    }
    if (donor.operators.length !== target.operators.length) {
      return {outcome: 'state-differ', stateEntries: 0};
    }
    let stateEntries = 0;
    let extraDonorEntries = 0;
    for (let i = 0; i < donor.operators.length; i++) {
      const expected = this.#readState(donor.operators[i], stateEntries);
      const actual = this.#readState(target.operators[i], stateEntries);
      if (expected === undefined || actual === undefined) {
        return {outcome: 'too-large', stateEntries};
      }
      stateEntries += actual.size;
      for (const [key, value] of actual) {
        const other = expected.get(key);
        if (other === undefined || !deepEqual(value, other)) {
          return {
            outcome: 'state-differ',
            stateEntries,
            difference: {operator: i, key, donor: other, target: value},
          };
        }
      }
      extraDonorEntries += expected.size - actual.size;
    }
    return extraDonorEntries > 0
      ? {outcome: 'donor-has-extra-state', stateEntries, extraDonorEntries}
      : {outcome: 'match', stateEntries};
  }

  /** @returns undefined if the entries would go over the limit. */
  #readState(
    operator: Pick<Storage, 'scan'>,
    entriesSoFar: number,
  ): Map<string, ReadonlyJSONValue> | undefined {
    const state = new Map<string, ReadonlyJSONValue>();
    for (const [key, value] of operator.scan()) {
      if (entriesSoFar + state.size >= this.#maxStateEntries) {
        return undefined;
      }
      state.set(key, value as ReadonlyJSONValue);
    }
    return state;
  }
}
