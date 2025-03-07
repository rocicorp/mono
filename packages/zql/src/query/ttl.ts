export type TimeUnit = 's' | 'm' | 'h' | 'd' | 'y';

/**
 * Time To Live. This is used for query expiration.
 * - `forever` means the query will never expire.
 * - `none` means the query will expire immediately.
 * - A number means the query will expire after that many milliseconds.
 * - A negative number means the query will never expire, this is same as 'forever'.
 * - A string like `1s` means the query will expire after that many seconds.
 * - A string like `1m` means the query will expire after that many minutes.
 * - A string like `1h` means the query will expire after that many hours.
 * - A string like `1d` means the query will expire after that many days.
 * - A string like `1y` means the query will expire after that many years.
 */
export type TTL = `${number}${TimeUnit}` | 'forever' | 'none' | number;

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
  if (ttl === 'forever') {
    return -1;
  }
  const multi = multiplier[ttl[ttl.length - 1] as TimeUnit];
  return Number(ttl.slice(0, -1)) * multi;
}
