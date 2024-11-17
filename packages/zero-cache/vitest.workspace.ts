import {defineWorkspace} from 'vitest/config';

const pgConfigForVersion = (version: number) => ({
  extends: './vitest.config.js',
  test: {
    name: `pg-${version}`,
    include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
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
]);
