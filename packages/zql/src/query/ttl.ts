export type TimeUnit = 's' | 'm';

/**
 * Time To Live. This is used for query expiration.
 * - `none` means the query will expire immediately.
 * - A number means the query will expire after that many milliseconds.
 * - A string like `1s` means the query will expire after that many seconds.
 * - A string like `1m` means the query will expire after that many minutes.
 *
 * TTL is capped at 5 minutes. Ideally, TTLs are not required
 * and can be avoided through query management.
 *
 * Query Management: a query with no TTL is open as long as some component has it open.
 * If a query should be around for the lifetime of the application, it should
 * be placed high enough in the component hierarchy such that it does
 * not unmount.
 *
 * E.g.,
 * - a query that every route needs would be in the root component
 * - a query that all components within a route need, but other routes do not, would be in the route component
 * - a truly ephemeral query that is only needed for a single component would be in that component
 */
export type TTL = `${number}${TimeUnit}` | 'none' | number;

export const DEFAULT_TTL: TTL = 'none';

const multiplier = {
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
  y: 365 * 24 * 60 * 60 * 1000,
} as const;

export function parseTTL(ttl: TTL): number {
  if (typeof ttl === 'number') {
    return Number.isNaN(ttl) ? 0 : !Number.isFinite(ttl) || ttl < 0 ? -1 : ttl;
  }
  if (ttl === 'none') {
    return 0;
  }
  const multi = multiplier[ttl[ttl.length - 1] as TimeUnit];
  const ttlInMs = Number(ttl.slice(0, -1)) * multi;
  return Math.min(ttlInMs, 5 * 60 * 1000); // Cap at 5 minutes
}

export function compareTTL(a: TTL, b: TTL): number {
  const ap = parseTTL(a);
  const bp = parseTTL(b);
  if (ap === -1 && bp !== -1) {
    return 1;
  }
  if (ap !== -1 && bp === -1) {
    return -1;
  }
  return ap - bp;
}

export function normalizeTTL(ttl: TTL): TTL {
  if (typeof ttl === 'string') {
    return ttl;
  }

  if (ttl < 0) {
    return 'none';
  }

  if (ttl === 0) {
    return 'none';
  }

  let shortest = ttl.toString();
  const lengthOfNumber = shortest.length;
  for (const unit of ['y', 'd', 'h', 'm', 's'] as const) {
    const multi = multiplier[unit];
    const value = ttl / multi;
    const candidate = `${value}${unit}`;
    if (candidate.length < shortest.length) {
      shortest = candidate;
    }
  }

  return (shortest.length < lengthOfNumber ? shortest : ttl) as TTL;
}
