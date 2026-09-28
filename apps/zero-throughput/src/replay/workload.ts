import {readFile} from 'node:fs/promises';
import type {ReadonlyJSONValue} from '../../../../packages/shared/src/json.ts';
import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';

/** A named (custom) query as a client registers it. */
export type WorkloadQuery = {
  readonly name: string;
  readonly args: readonly ReadonlyJSONValue[];
  readonly ttlMs: number;
};

/** A screen-scoped query, with how often live groups held it. */
export type ScreenQuery = WorkloadQuery & {
  readonly weight: number;
};

/**
 * One client group from the CVR snapshot. A replayed session registers
 * `sessionQueries`, in order, when it connects.
 */
export type WorkloadGroup = {
  readonly sourceClientGroupID: string;
  readonly userID: string;
  readonly sessionQueries: readonly WorkloadQuery[];
};

export type WorkloadNameStats = {
  readonly name: string;
  readonly groups: number;
  readonly desires: number;
  readonly kind: 'session' | 'screen';
};

export type WorkloadStats = {
  readonly snapshot: string;
  readonly liveWindowSeconds: number;
  readonly liveGroups: number;
  readonly groupsWithoutUser: number;
  /** Groups whose connection was granted in the hour before the snapshot. */
  readonly connectsLastHour: number;
  /** Little's law: live groups / connect rate. */
  readonly meanSessionSeconds: number;
  /** Screen registrations per minute of session, from held screen desires. */
  readonly screenQueriesPerSessionMinute: number;
  readonly sessionQueriesPerGroup: {
    readonly p50: number;
    readonly p90: number;
    readonly max: number;
  };
  readonly names: readonly WorkloadNameStats[];
};

export type ReplayWorkload = {
  readonly formatVersion: 1;
  readonly source: {
    readonly cvrDir: string;
    readonly generatedAt: string;
  };
  readonly clientSchema: ClientSchema;
  readonly groups: readonly WorkloadGroup[];
  readonly screenQueries: readonly ScreenQuery[];
  readonly stats: WorkloadStats;
};

export async function loadWorkload(path: string): Promise<ReplayWorkload> {
  const workload = JSON.parse(await readFile(path, 'utf8')) as ReplayWorkload;
  if (workload.formatVersion !== 1) {
    throw new Error(
      `${path}: unsupported workload formatVersion ${String(workload.formatVersion)}`,
    );
  }
  if (workload.groups.length === 0) {
    throw new Error(`${path}: the workload has no client groups`);
  }
  return workload;
}

const DEFAULT_USER_ID_KEYS = ['userId', 'userID', 'user_id'];

/**
 * The CVR does not record which user a client group authenticated as, but the
 * user's own queries carry it as an argument. Returns the most frequent
 * string that appears either as a whole top-level argument or under one of
 * `userIDKeys` in an object argument, optionally only strings matching
 * `pattern` (other string arguments, like a country code, can outnumber the
 * user ID in a small group).
 */
export function inferUserID(
  queries: readonly {readonly args: readonly ReadonlyJSONValue[]}[],
  userIDKeys: readonly string[] = DEFAULT_USER_ID_KEYS,
  pattern?: RegExp | undefined,
): string | undefined {
  const counts = new Map<string, number>();
  const count = (value: ReadonlyJSONValue | undefined) => {
    if (
      typeof value === 'string' &&
      value !== '' &&
      (pattern === undefined || pattern.test(value))
    ) {
      counts.set(value, (counts.get(value) ?? 0) + 1);
    }
  };
  for (const {args} of queries) {
    for (const arg of args) {
      if (typeof arg === 'string') {
        count(arg);
      } else if (
        arg !== null &&
        typeof arg === 'object' &&
        !Array.isArray(arg)
      ) {
        const record = arg as Readonly<Record<string, ReadonlyJSONValue>>;
        for (const key of userIDKeys) {
          count(record[key]);
        }
      }
    }
  }
  let best: string | undefined;
  let bestCount = 0;
  for (const [value, n] of counts) {
    if (
      n > bestCount ||
      (n === bestCount && best !== undefined && value < best)
    ) {
      best = value;
      bestCount = n;
    }
  }
  return best;
}

/**
 * Splits query names into those a session registers when it connects and
 * those it registers while navigating. A name is a session query when it is
 * listed explicitly or when at least `threshold` of the live groups hold it.
 */
export function classifyQueryNames(
  groupsHoldingName: ReadonlyMap<string, number>,
  liveGroups: number,
  threshold: number,
  sessionNames: ReadonlySet<string>,
): Map<string, 'session' | 'screen'> {
  const kinds = new Map<string, 'session' | 'screen'>();
  for (const [name, groups] of groupsHoldingName) {
    kinds.set(
      name,
      sessionNames.has(name) || groups >= threshold * liveGroups
        ? 'session'
        : 'screen',
    );
  }
  return kinds;
}

const HOUR_OFFSET = /([+-]\d\d)$/;
const HOUR_MINUTE_OFFSET = /([+-]\d\d)(\d\d)$/;

/** Parses Postgres `timestamptz` text such as `2026-09-26 04:35:22.9+00`. */
export function parseTimestamp(text: string | undefined): number | undefined {
  if (text === undefined || text === '') {
    return undefined;
  }
  const iso = text
    .replace(' ', 'T')
    .replace(HOUR_OFFSET, '$1:00')
    .replace(HOUR_MINUTE_OFFSET, '$1:$2');
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : undefined;
}
