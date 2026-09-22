import {describe, expect, test} from 'vitest';
import {
  ChangeLogTransactionHasher,
  type ChangeLogHashRow,
} from './change-log-transaction-hash.ts';

const rows: readonly ChangeLogHashRow[] = [
  {
    watermark: '06',
    pos: 0,
    tag: 'begin',
    change: '{"tag":"begin"}',
    precommit: null,
  },
  {
    watermark: '06',
    pos: 1,
    tag: 'commit',
    change: '{"tag":"commit"}',
    precommit: '06',
  },
];

function hash(transactionRows: readonly ChangeLogHashRow[]): string {
  const hasher = new ChangeLogTransactionHasher();
  transactionRows.forEach(row => hasher.add(row));
  return hasher.digest();
}

describe('change-log transaction hash', () => {
  test('is stable for identical rows', () => {
    expect(hash(rows)).toBe(hash(rows));
    expect(hash(rows)).toHaveLength(64);
  });

  test.each(['watermark', 'pos', 'tag', 'change', 'precommit'] as const)(
    'detects a changed %s',
    field => {
      const changed = rows.map((row, index) =>
        index === 1
          ? {
              ...row,
              [field]:
                field === 'pos'
                  ? 2
                  : field === 'precommit'
                    ? null
                    : `${row[field]}-changed`,
            }
          : row,
      );
      expect(hash(changed)).not.toBe(hash(rows));
    },
  );

  test('detects reordered rows and unambiguous field boundaries', () => {
    expect(hash(rows.toReversed())).not.toBe(hash(rows));
    expect(hash([{...rows[0], tag: 'ab', change: 'c'}, rows[1]])).not.toBe(
      hash([{...rows[0], tag: 'a', change: 'bc'}, rows[1]]),
    );
  });
});
