// zero-pg is deprecated in favor of zero-server.
// Export all the things from zero-server for backwards compatibility until people have stopped using zero-pg.

// eslint-disable-next-line no-restricted-imports
export * from '../../zero-server/src/mod.ts';
export * from '../../zero-server/src/adapters/postgresjs.ts';
