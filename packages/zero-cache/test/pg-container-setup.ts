import {PostgreSqlContainer} from '@testcontainers/postgresql';

export function runPostgresContainer(image: string, timezone: string) {
  return async ({provide}) => {
    const container = await new PostgreSqlContainer(image)
      .withCommand([
        'postgres',
        '-c',
        'wal_level=logical',
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
