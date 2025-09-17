/* eslint-disable @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/require-await, @typescript-eslint/no-unused-vars, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-base-to-string, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/unbound-method, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, require-await, no-unused-private-class-members, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable */
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
