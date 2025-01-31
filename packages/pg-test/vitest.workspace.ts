import {defineWorkspace} from 'vitest/config';

const pgConfigForVersion = (version: number) => ({
  extends: './vitest.config.js',
  test: {
    name: `pg-${version}`,
    // going up a directory is intentional as this workspace config is shared by other packages
    include: ['../pg-test/src/**/*.pg-test.?(c|m)[jt]s?(x)'],
    globalSetup: [`./test/pg-${version}.ts`],
  },
});

export default defineWorkspace([
  {
    extends: './vitest.config.js',
    test: {
      name: 'no-pg',
      include: ['src/**/*.test.?(c|m)[jt]s?(x)'],
    },
  },
  pgConfigForVersion(15),
  pgConfigForVersion(16),
  pgConfigForVersion(17),
  // To run tests against a custom Postgres instance (e.g. Aurora), specify
  // the connection string in the CUSTOM_PG environment variable, and optionally
  // limit the test runner to the "custom-pg" project:
  //
  // CUSTOM_PG=postgresql://... npm run test -- --project custom-pg
  ...(process.env['CUSTOM_PG']
    ? [
        {
          extends: './vitest.config.js',
          test: {
            name: 'custom-pg',
            include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
            provide: {
              // Referenced by ./src/test/db.ts
              pgConnectionString: process.env['CUSTOM_PG'],
            },
          },
        },
      ]
    : []),
]);
