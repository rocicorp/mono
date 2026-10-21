import {test} from 'vitest';
import {
  clampTTL,
  compareTTL,
  normalizeTTL,
  parseTTL,
  type TTL,
} from '../../../zql/src/query/ttl.ts';
import {emitVectors} from '../vectors.ts';

test('ttl', () => {
  const ttls: TTL[] = [
    'none',
    'forever',
    0,
    -1,
    -100,
    1,
    999,
    1000,
    1500,
    60_000,
    90_000,
    3_600_000,
    86_400_000,
    31_536_000_000,
    600_000,
    600_001,
    '0s',
    '1s',
    '1.5s',
    '30s',
    '5m',
    '10m',
    '11m',
    '1h',
    '1d',
    '1y',
    '0.5h',
    '-1m',
    ' 2m',
  ];
  emitVectors(
    'ttl',
    'ttl',
    'parseTTL, normalizeTTL, and clampTTL; plus compareTTL against every other case (signs).',
    ttls.map(ttl => ({
      ttl,
      parsed: parseTTL(ttl),
      normalized: normalizeTTL(ttl),
      clamped: clampTTL(ttl),
      compare: ttls.map(other => Math.sign(compareTTL(ttl, other))),
    })),
  );
});
