import {PostgreSqlContainer} from '@testcontainers/postgresql';

export function runPostgresContainer(image: string) {
  return async ({provide}) => {
    const container = await new PostgreSqlContainer(image)
      .withCommand(['postgres', '-c', 'wal_level=logical'])
      .start();

    // Referenced by ./src/test/db.ts
    provide('pgContainerConnectionString', container.getConnectionUri());

    return async () => {
      await container.stop();
    };
  };
}
