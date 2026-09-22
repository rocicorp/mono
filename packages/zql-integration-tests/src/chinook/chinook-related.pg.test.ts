// oxlint-disable valid-describe-callback
// oxlint-disable expect-expect
import {describe, test} from 'vitest';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {createVitests} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

// The `table.related(...)` cases for every table and relationship. Split out of
// chinook.pg.test.ts because they are the slowest cases in that suite, so CI
// can place them in a different test shard.

// Junction edges do not correctly handle limits in ZQL
// oxlint-disable-next-line unicorn/prefer-set-has -- Keep as array for consistency with existing code
const brokenRelationshipLimits = ['tracks', 'customer', 'playlists'];

const pgContent = await getChinook();
const tables = Object.keys(schema.tables) as Array<keyof typeof schema.tables>;

describe(
  'Chinook PG related Tests',
  {
    timeout: 30_000,
  },
  async () => {
    test.each(
      await createVitests(
        {
          suiteName: 'compiler_chinook_related',
          pgContent,
          zqlSchema: schema,
        },
        // table.related('relationship')
        (() =>
          tables.flatMap(table =>
            getRelationships(table).map(
              relationship =>
                ({
                  name: `${table}.related('${relationship}')`,
                  createQuery: q =>
                    (q[table] as AnyQuery).related(relationship),
                }) as const,
            ),
          ))(),
        // table.related('relationship', q => q.limit(100))
        (() =>
          tables.flatMap(table =>
            getRelationships(table)
              .filter(r => !brokenRelationshipLimits.includes(r))
              .map(
                relationship =>
                  ({
                    name: `${table}.related('${relationship}', q => q.limit(100))`,
                    createQuery: q =>
                      (q[table] as AnyQuery).related(relationship, q =>
                        q.limit(100),
                      ),
                  }) as const,
              ),
          ))(),
      ),
    )('$name', async ({fn}) => {
      await fn();
    });
  },
);

function getRelationships(table: string) {
  return Object.keys(
    (schema.relationships as Record<string, Record<string, unknown>>)[table] ??
      {},
  );
}
