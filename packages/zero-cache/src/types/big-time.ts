/**
 * TIMESTAMPTZ objects are internally represented in PostgreSQL as 64-bit
 * integers and returned by the `pg-logical-replication` package as bigints.
 * To stores these values in TIMESTAMPTZ columns, the bigints must be
 * converted to string values that PostgreSQL automatically casts to
 * a TIMESTAMPTZ object.
 */
export function epochMicrosToTimestampTz(epochMicros: bigint): string {
  // Get millisecond part
  const epochMillis = epochMicros / 1000n;
  // Get microsecond part - keep leading zeros
  const micros = String(epochMicros).slice(-3);
  // Get ISO 8601 timestamp
  const isoDate = new Date(Number(epochMillis)).toISOString();
  // Add in microseconds
  return isoDate.replace('Z', micros + 'Z');
}
