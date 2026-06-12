import {PostgreSqlContainer} from '@testcontainers/postgresql';

const IMAGE_MAJOR_VERSION = /^postgres:(\d+)/;

export function runPostgresContainer(image: string, timezone: string) {
  return async ({provide}) => {
    // TEST_PG_<major> (e.g. TEST_PG_16=postgres://user:password@pg16:5432/postgres)
    // points the tests at an already-running Postgres instead of starting one
    // with testcontainers. The instance must be configured with the same
    // server settings as the container started below. This is how the dev
    // container runs the pg tests without a Docker daemon; see
    // .devcontainer/docker-compose.yml.
    const major = image.match(IMAGE_MAJOR_VERSION)?.[1];
    const externalUri = major && process.env[`TEST_PG_${major}`];
    if (externalUri) {
      provide('pgConnectionString', externalUri);
      provide('pgImage', image);
      provide('pgTimezone', timezone);
      return;
    }

    const container = await new PostgreSqlContainer(image)
      .withCommand([
        'postgres',
        '-c',
        'wal_level=logical',
        '-c',
        'max_replication_slots=100',
        '-c',
        'max_wal_senders=100',
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
