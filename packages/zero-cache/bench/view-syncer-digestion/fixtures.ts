import {envInt} from './perf-utils.ts';
import type {DigestScenario} from './types.ts';

export const APP_ID = 'bench';
export const INITIAL_VERSION = '000000000001';
export const ACTIVE_TABLE = 'active_rows';
export const BACKGROUND_TABLE = 'background_rows';

export const TABLES = {
  active: ACTIVE_TABLE,
  background: BACKGROUND_TABLE,
} as const;

export function loadScenarios(): DigestScenario[] {
  const transactions = envInt('ZERO_VS_DIGESTION_TX', 1_000);
  const rowsPerTransaction = envInt('ZERO_VS_DIGESTION_ROWS_PER_TX', 10);

  return [
    {
      name: 'unobserved-heavy',
      transactions,
      rowsPerTransaction,
      activeRowsPerTransaction: 0,
    },
    {
      name: 'mixed-10pct-active',
      transactions,
      rowsPerTransaction,
      activeRowsPerTransaction: Math.max(
        1,
        Math.floor(rowsPerTransaction / 10),
      ),
    },
    {
      name: 'all-active',
      transactions,
      rowsPerTransaction,
      activeRowsPerTransaction: rowsPerTransaction,
    },
  ];
}

export function watermark(index: number): string {
  return index.toString(16).padStart(12, '0');
}
