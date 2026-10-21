/**
 * `zero-swift drift record`: runs an app's TS queries and mutators on
 * samples and records what they did. The Swift drift test
 * (`Drift.failures` in ZeroTesting) checks the app's Swift port against the
 * record, so a change to either side that the other lacks fails a test.
 *
 * The samples module exports `schema`, the `queries` and `mutators`
 * registries, and `querySamples` and `mutatorSamples`.
 */
import {pathToFileURL} from 'node:url';
import {
  createBuilder,
  defineMutator,
  defineMutators,
  isMutator,
  isQuery,
  mustGetMutator,
  mustGetQuery,
  Zero,
  type QueryInternals,
  type Schema,
} from '@rocicorp/zero';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {hashOfAST} from '../../zero-protocol/src/query-hash.ts';

type Row = Record<string, ReadonlyJSONValue>;

/** What the recorder uses of a `Zero`. */
type Client = {
  mutate(request: never): {client: Promise<unknown>};
  run(query: never): Promise<unknown>;
  close(): Promise<void>;
};

export type QuerySample = {
  /** The query's name in the registry, such as `issue.byID`. */
  name: string;
  /** Omit for a query without arguments. */
  args?: ReadonlyJSONValue | undefined;
  context?: ReadonlyJSONValue | undefined;
};

export type MutatorSample = {
  /** The mutator's name in the registry, such as `issue.create`. */
  name: string;
  /** Omit for a mutator without arguments. */
  args?: ReadonlyJSONValue | undefined;
  context?: ReadonlyJSONValue | undefined;
  /** The rows in the client before the mutator runs, by table. */
  seed?: Record<string, Row[]> | undefined;
};

export type DriftSamples = {
  schema: Schema;
  queries: object;
  mutators: object;
  querySamples: QuerySample[];
  mutatorSamples: MutatorSample[];
};

const seedName = '__drift.seed';
const dayMs = 24 * 60 * 60 * 1000;
const offlineWarning =
  'Zero starting up with no server URL. No data will be synced.';

/** Records the samples that the module at `path` exports by default. */
export async function recordDriftFile(path: string): Promise<string> {
  const module = (await import(pathToFileURL(path).href)) as {
    default?: DriftSamples;
  };
  if (!module.default?.querySamples || !module.default.mutatorSamples) {
    throw new Error(`${path} does not export drift samples by default`);
  }
  return recordDrift(module.default);
}

export async function recordDrift(samples: DriftSamples): Promise<string> {
  const {schema, queries, mutators} = samples;
  const querySamples = samples.querySamples.map(sample => {
    const query = mustGetQuery(queries as never, sample.name) as unknown as {
      fn(options: {args: unknown; ctx: unknown}): never;
    };
    const {ast} = query.fn({
      args: sample.args,
      ctx: sample.context,
    }) as unknown as QueryInternals<string, Schema, unknown>;
    return {
      name: sample.name,
      args: sample.args === undefined ? [] : [sample.args],
      context: sample.context ?? null,
      ast: json(ast),
      hash: hashOfAST(ast),
    };
  });

  const mutatorSamples = [];
  // Each sample's client is offline on purpose; Zero warns about that once
  // per client.
  const {warn} = console;
  // oxlint-disable-next-line no-console
  console.warn = (...args: unknown[]) => {
    if (args[0] !== offlineWarning) warn(...args);
  };
  try {
    for (const [i, sample] of samples.mutatorSamples.entries()) {
      const result = await runMutator(schema, mutators, sample, 0);
      // A mutator that reads the clock or makes up IDs does something else
      // in Swift, so it can't be compared.
      const again = await runMutator(schema, mutators, sample, dayMs);
      if (JSON.stringify(again) !== JSON.stringify(result)) {
        throw new Error(
          `mutator ${sample.name} sample ${i} does something else when run ` +
            `again a day later. Pass IDs and timestamps as arguments.`,
        );
      }
      mutatorSamples.push({
        ...sample,
        context: sample.context ?? null,
        seed: sample.seed ?? {},
        ...result,
      });
    }
  } finally {
    // oxlint-disable-next-line no-console
    console.warn = warn;
  }

  const vectors = {
    queries: names(queries, isQuery, q => q.queryName),
    mutators: names(mutators, isMutator, m => m.mutatorName),
    querySamples,
    mutatorSamples,
  };
  return JSON.stringify(vectors, null, 2) + '\n';
}

/** Puts the sample's rows before the mutator runs. */
async function seed({
  tx,
  args,
}: {
  tx: {mutate: Record<string, {upsert(row: Row): Promise<void>}>};
  args: Record<string, Row[]>;
}) {
  for (const [table, rows] of Object.entries(args)) {
    for (const row of rows) {
      await tx.mutate[table].upsert(row);
    }
  }
}

/**
 * Runs one mutator in a client of its own, offline, with `Date.now` moved
 * `shiftMs` ahead while the mutator runs.
 */
async function runMutator(
  schema: Schema,
  mutators: object,
  sample: MutatorSample,
  shiftMs: number,
) {
  const registry = defineMutators(mutators as never, {
    __drift: {seed: defineMutator(seed as never)},
  });
  const zero = new Zero({
    schema,
    mutators: registry,
    userID: 'drift',
    storageKey: `drift-${Math.random().toString(36).slice(2)}`,
    kvStore: 'mem',
    context: sample.context,
    // The mutators' errors are what the samples record, not news.
    logSink: {log() {}},
  } as never) as unknown as Client;
  try {
    const call = (name: string, args: unknown) =>
      zero.mutate(
        (
          mustGetMutator(registry, name) as unknown as (
            ...a: unknown[]
          ) => never
        )(...(args === undefined ? [] : [args])),
      );
    await call(seedName, sample.seed ?? {}).client;
    const before = await rows(zero, schema);
    const {now} = Date;
    Date.now = () => now() + shiftMs;
    let outcome;
    try {
      outcome = await call(sample.name, sample.args).client;
    } finally {
      Date.now = now;
    }
    const after = await rows(zero, schema);
    return {outcome: json(outcome), changes: diff(schema, before, after)};
  } finally {
    await zero.close();
  }
}

/** Every row of every table, by table and then by primary key. */
async function rows(zero: Client, schema: Schema) {
  const builder = createBuilder(schema) as unknown as Record<string, never>;
  const out = new Map<string, Map<string, Row>>();
  for (const [table, {primaryKey}] of Object.entries(schema.tables)) {
    const data = (await zero.run(builder[table])) as unknown as Row[];
    out.set(
      table,
      new Map(
        data.map(row => [
          JSON.stringify(primaryKey.map(c => row[c] ?? null)),
          json(row) as Row,
        ]),
      ),
    );
  }
  return out;
}

/**
 * The rows the mutator put and deleted: by table in schema order, then by
 * primary key in UTF-8 order.
 */
function diff(
  schema: Schema,
  before: Map<string, Map<string, Row>>,
  after: Map<string, Map<string, Row>>,
) {
  const changes = [];
  for (const [table, {primaryKey}] of Object.entries(schema.tables)) {
    const old = before.get(table) ?? new Map<string, Row>();
    const now = after.get(table) ?? new Map<string, Row>();
    const keys = [...new Set([...old.keys(), ...now.keys()])].toSorted((a, b) =>
      Buffer.compare(Buffer.from(a), Buffer.from(b)),
    );
    for (const key of keys) {
      const o = old.get(key);
      const n = now.get(key);
      if (n !== undefined) {
        if (o === undefined || !sameJSON(o, n)) {
          changes.push({table, op: 'put', row: n});
        }
      } else if (o !== undefined) {
        changes.push({
          table,
          op: 'del',
          id: Object.fromEntries(primaryKey.map(c => [c, o[c] ?? null])),
        });
      }
    }
  }
  return changes;
}

function sameJSON(a: ReadonlyJSONValue, b: ReadonlyJSONValue): boolean {
  if (a === b) return true;
  if (typeof a !== 'object' || typeof b !== 'object' || !a || !b) return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((v, i) => sameJSON(v, b[i]))
    );
  }
  const ak = Object.keys(a);
  const bo = b as Record<string, ReadonlyJSONValue>;
  return (
    ak.length === Object.keys(b).length &&
    ak.every(
      k =>
        k in bo && sameJSON((a as Record<string, ReadonlyJSONValue>)[k], bo[k]),
    )
  );
}

/** The names of the registry's leaves, sorted. */
function names<T>(
  registry: object,
  isLeaf: (v: unknown) => v is T,
  name: (leaf: T) => string,
): string[] {
  const out: string[] = [];
  const walk = (node: unknown) => {
    if (isLeaf(node)) {
      out.push(name(node));
    } else if (node && typeof node === 'object') {
      for (const [key, child] of Object.entries(node)) {
        if (key !== '~') walk(child);
      }
    }
  };
  walk(registry);
  return out.sort();
}

function json(v: unknown): ReadonlyJSONValue {
  return v === undefined ? null : JSON.parse(JSON.stringify(v));
}
