// Type definitions and imports for @op-engineering/op-sqlite
// This file isolates the module resolution workarounds needed for this package

import {open as openDB} from '@op-engineering/op-sqlite';

/**
 * Result of `executeRaw`/`executeRawSync`. op-sqlite changed this shape in
 * v17.0.0 (commit `4cd58b8`): `<=16` returned a bare array of row arrays, `>=17`
 * returns `{rowsAffected, insertId, rawRows, columnNames}` with rows under
 * `rawRows`. The peer range is `>=15`, so both shapes must be handled.
 */
export type RawResult =
  | unknown[][]
  | {
      rawRows?: unknown[][];
      columnNames?: unknown[];
      rowsAffected?: number;
      insertId?: number;
    };

/** Extracts the row arrays from either {@link RawResult} shape. */
export function rawResultRows(result: RawResult): unknown[][] {
  return (
    Array.isArray(result) ? result : (result.rawRows ?? [])
  ) as unknown[][];
}

// Minimal type definitions for @op-engineering/op-sqlite
// These types are used as fallback since imports have module resolution issues
export interface DB {
  close: () => void;
  delete: (location?: string) => void;
  executeRaw: (query: string, params?: string[]) => Promise<RawResult>;
  executeRawSync: (query: string, params?: string[]) => RawResult;
}

export type OpenFunction = (params: {
  name: string;
  location?: string;
  encryptionKey?: string;
}) => DB;

// Export the open function with proper typing
export const open: OpenFunction = openDB;
