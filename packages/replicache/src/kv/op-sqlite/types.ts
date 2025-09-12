// Type definitions and imports for @op-engineering/op-sqlite
// This file isolates the module resolution workarounds needed for this package

// @ts-expect-error - Module resolution issue with @op-engineering/op-sqlite exports
import {open as openDB} from '@op-engineering/op-sqlite';

// Minimal type definitions for @op-engineering/op-sqlite
// These types are used as fallback since imports have module resolution issues
export interface DB {
  close: () => void;
  delete: (location?: string) => void;
  executeRaw: (query: string, params?: string[]) => Promise<string[][]>;
  executeRawSync: (query: string, params?: string[]) => string[][];
}

export type OpenFunction = (params: {
  name: string;
  location?: string;
  encryptionKey?: string;
}) => DB;

// Export the open function with proper typing
export const open: OpenFunction = openDB;
