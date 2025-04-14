import {PostgreSqlContainer} from '@testcontainers/postgresql';

export function runPostgresContainer(image: string) {
  return async ({provide}) => {
    const container = await new PostgreSqlContainer(image)
      .withCommand([
        'postgres',
        '-c',
        'wal_level=logical',
        '-c',
        'timezone=UTC+1',
      ])
      .start();

    // Referenced by ./src/test/db.ts
    provide('pgConnectionString', container.getConnectionUri());

    return async () => {
      await container.stop();
    };
  };
}
