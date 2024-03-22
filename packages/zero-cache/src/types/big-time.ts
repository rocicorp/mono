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
