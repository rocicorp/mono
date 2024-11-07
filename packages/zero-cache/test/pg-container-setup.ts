import {PostgreSqlContainer} from '@testcontainers/postgresql';

// TODO: Pass this in an ENV variable or some appropriate config channel
//       to run tests with different versions of Postgres.
const PG_IMAGE = 'postgres:16.3-alpine3.19';

export default async function ({provide}) {
  const container = await new PostgreSqlContainer(PG_IMAGE)
    .withCommand(['postgres', '-c', 'wal_level=logical'])
    .start();

  // Referenced by ./src/test/db.ts
  provide('pgContainerConnectionString', container.getConnectionUri());

  return async () => {
    await container.stop();
  };
}
