import {PostgreSqlContainer} from '@testcontainers/postgresql';

export function runPostgresContainer(image: string, timezone: string) {
  return async ({provide}) => {
    const container = await new PostgreSqlContainer(image)
      .withCommand([
        'postgres',
        '-c',
        'wal_level=logical',
        '-c',
        'max_replication_slots=100',
        '-c',
        `timezone=${timezone}`,
      ])
      .start();

    // Referenced by ./src/test/db.ts
    provide('pgConnectionString', container.getConnectionUri());
    provide('pgImage', image);
    provide('pgTimezone', timezone);

    return async () => {
      await container.stop();
    };
  };
}
