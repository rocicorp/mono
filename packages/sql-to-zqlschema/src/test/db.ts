import {inject} from 'vitest';

declare module 'vitest' {
  export interface ProvidedContext {
    pgConnectionString: string;
  }
}

/**
 * Gets the PostgreSQL connection string from vitest's provided context.
 * This is set up by the global setup file (pg-17.ts from zero-cache).
 */
export function getConnectionString(): string {
  const connectionString = inject('pgConnectionString');
  if (!connectionString) {
    throw new Error(
      'pgConnectionString not provided. Test file must have suffix ".pg.test.ts" to run in PostgreSQL environment.',
    );
  }
  return connectionString;
}
