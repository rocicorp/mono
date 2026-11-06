import type {LogContext} from '@rocicorp/logger';
import {h64} from '../../../../shared/src/hash.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {
  normalizeClientSchema,
  type ClientSchema,
} from '../../../../zero-protocol/src/client-schema.ts';
import type {Storage} from '../../../../zql/src/ivm/operator.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';

/** Past this, comparing one pipeline would hold up the worker too long. */
const DEFAULT_MAX_STATE_ENTRIES = 1_000_000;

/** Pipelines can only be alike if their row keys are, which this determines. */
export function clientSchemaKey(schema: ClientSchema): string {
  return h64(JSON.stringify(normalizeClientSchema(schema))).toString(36);
}

export type PipelineStateSource = {
  readonly version: () => string;
  /** Whether the pipeline's state is in the middle of changing. */
  readonly busy: () => boolean;
  readonly rowSetSignature: () => bigint | undefined;
  /** In the order they were built, which identical plans share. */
  readonly operators: readonly Pick<Storage, 'scan'>[];
};

export type CloneShadowResult = {
  readonly outcome:
    | 'no-donor'
    | 'version-mismatch'
    | 'donor-busy'
    | 'match'
    | 'donor-has-extra-state'
    | 'rows-differ'
    | 'state-differ'
    | 'too-large';
  readonly stateEntries: number;
  readonly compareTimeMs: number;
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
 * Measures whether a sync worker could skip hydrating a query by copying the
 * result from another client group's pipeline, without doing so: each group
 * still hydrates, and its result is compared with what would have been copied.
 *
 * A pipeline's identity is `(clientSchemaKey, transformationHash)`, and it can
 * only donate at the version the new one hydrated at. The comparison includes
 * donors that reached that version by advancing rather than hydrating, because
 * a copy is only sound if the two ways of getting there leave compatible state.
 *
 * Advancing can leave entries behind for rows the pipeline has touched, which a
 * fresh hydration would not create. Those are reported separately from real
 * differences, because a copy takes after the donor rather than a hydration.
 *
 * Only operator storage is compared. A pipeline's join indexes hold the rows it
 * emitted, so matching rows imply matching indexes.
 */
export class CloneShadow {
  readonly #lc: LogContext;
  readonly #maxStateEntries: number;
  readonly #onResult: ((result: CloneShadowResult) => void) | undefined;
  readonly #sources = new Map<string, Set<PipelineStateSource>>();
  readonly #comparisons = getOrCreateCounter(
    'sync',
    'clone-shadow',
    'Hydrations compared with a copy from another client group, by outcome',
  );
  readonly #matchedHydrationMs = getOrCreateCounter(
    'sync',
    'clone-shadow-matched-hydration-ms',
    'Time spent hydrating queries whose result matched a possible copy',
  );

  constructor({
    lc,
    maxStateEntries = DEFAULT_MAX_STATE_ENTRIES,
    onResult,
  }: {
    lc: LogContext;
    maxStateEntries?: number | undefined;
    onResult?: ((result: CloneShadowResult) => void) | undefined;
  }) {
    this.#lc = lc.withContext('component', 'clone-shadow');
    this.#maxStateEntries = maxStateEntries;
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

  compare(
    key: string,
    target: PipelineStateSource,
    hydrationTimeMs: number,
  ): CloneShadowResult {
    const start = performance.now();
    const compared = this.#compare(key, target);
    const {outcome} = compared;
    const result = {...compared, compareTimeMs: performance.now() - start};
    this.#comparisons.add(1, {outcome});
    if (outcome === 'match') {
      this.#matchedHydrationMs.add(hydrationTimeMs);
    }
    const log =
      outcome === 'rows-differ' || outcome === 'state-differ'
        ? this.#lc.warn
        : this.#lc.debug;
    log?.call(this.#lc, `clone shadow: ${outcome}`, {
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
  ): Omit<CloneShadowResult, 'compareTimeMs'> {
    const version = target.version();
    let outcome: CloneShadowResult['outcome'] = 'no-donor';
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
  ): Omit<CloneShadowResult, 'compareTimeMs'> {
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
