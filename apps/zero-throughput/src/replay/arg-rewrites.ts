import type {ReadonlyJSONValue} from '../../../../packages/shared/src/json.ts';
import type {WorkloadQuery} from './workload.ts';

/**
 * Replaces a recorded time-valued argument with the current time, so a query
 * whose client derives an argument from the clock (e.g. "now, floored to the
 * hour") gets a new identity when that value would change, as it does for a
 * real client. Without a rewrite the snapshot's value is replayed as-is.
 */
export type ArgRewrite = {
  readonly name: string;
  readonly key: string;
  readonly unit: TimeUnit;
};

const UNITS = {now: 1, minute: 60_000, hour: 3_600_000, day: 86_400_000};

export type TimeUnit = keyof typeof UNITS;

/**
 * Parses `name.key=unit` entries (comma-separated or repeated), e.g.
 * `homeStories.now=hour`. The unit is `now` (epoch ms) or the start of the
 * current UTC `minute`, `hour` or `day`.
 */
export function parseArgRewrites(specs: readonly string[]): ArgRewrite[] {
  return specs
    .flatMap(s => s.split(','))
    .map(s => s.trim())
    .filter(Boolean)
    .map(spec => {
      const eq = spec.indexOf('=');
      const dot = spec.indexOf('.');
      const unit = spec.slice(eq + 1);
      if (dot <= 0 || eq <= dot + 1 || !(unit in UNITS)) {
        throw new Error(
          `Invalid --arg-rewrite "${spec}"; expected name.key=${Object.keys(UNITS).join('|')}`,
        );
      }
      return {
        name: spec.slice(0, dot),
        key: spec.slice(dot + 1, eq),
        unit: unit as TimeUnit,
      };
    });
}

export function currentValue(unit: TimeUnit, nowMs: number): number {
  const size = UNITS[unit];
  return Math.floor(nowMs / size) * size;
}

/** Sets `key` in each object argument of a matching query. */
export function rewriteArgs(
  query: WorkloadQuery,
  rewrites: readonly ArgRewrite[],
  nowMs: number,
): WorkloadQuery {
  const matching = rewrites.filter(r => r.name === query.name);
  if (matching.length === 0) {
    return query;
  }
  const args = query.args.map(arg => {
    if (arg === null || typeof arg !== 'object' || Array.isArray(arg)) {
      return arg;
    }
    const record = arg as Readonly<Record<string, ReadonlyJSONValue>>;
    let result = record;
    for (const {key, unit} of matching) {
      if (key in record) {
        result = {...result, [key]: currentValue(unit, nowMs)};
      }
    }
    return result;
  });
  return {...query, args};
}
