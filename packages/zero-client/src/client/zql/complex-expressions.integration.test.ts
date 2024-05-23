import {describe, expect, test} from 'vitest';
import {and, exp, or} from '../../../../zql/src/zql/query/entity-query.js';
import {bulkSet, newZero} from './integration-test-util.js';

describe('complex expressions', async () => {
  const z = newZero();
  const tracks = [
    {
      id: '001',
      title: 'a',
      albumId: '001',
      length: 100,
    },
    {
      id: '002',
      title: 'a',
      albumId: '001',
      length: 200,
    },
    {
      id: '003',
      title: 'a',
      albumId: '002',
      length: 300,
    },
    {
      id: '004',
      title: 'c',
      albumId: '002',
      length: 400,
    },
  ];
  await bulkSet(z, {
    tracks,
  });

  // TODO(mlaw): also test these in a way that we can validate the optimized query plan is being run.
  // We need to hook up some way of tracing execution of a query and inspecting the trace.
  test.each([
    {
      name: 'cursor style, asc.',
      // attempt to get the page after id001.
      // The cursor for a pagination after id001 would be the title and id of id001.
      // id002 has a duplicate title of id001 but a higher id so it should be included.
      query: () =>
        z.query.track
          .asc('title')
          .where(
            or(
              exp('title', '>', 'a'),
              and(exp('title', '=', 'a'), exp('id', '>', '001')),
            ),
          ),
      expected: tracks.slice(1),
    },
    {
      name: 'cursor style, asc, starting from unique (id003)',
      query: () =>
        z.query.track
          .asc('title')
          .where(
            or(
              exp('title', '>', 'a'),
              and(exp('title', '=', 'a'), exp('id', '>', '003')),
            ),
          ),
      expected: tracks.slice(3),
    },
    {
      name: 'cursor style, desc',
      // attempt to get the page before id003.
      query: () =>
        z.query.track
          .desc('title')
          .where(
            or(
              exp('title', '<', 'a'),
              and(exp('title', '=', 'a'), exp('id', '<', '003')),
            ),
          ),
      expected: tracks.slice(0, 2).reverse(),
    },
    {
      name: 'cursor style desc, starting from unique (id004)',
      query: () =>
        z.query.track
          .desc('title')
          .where(
            or(
              exp('title', '<', 'c'),
              and(exp('title', '=', 'c'), exp('id', '<', '004')),
            ),
          ),
      expected: tracks.slice(0, 3).reverse(),
    },
  ])('$name', async ({query, expected}) => {
    const stmt = query().prepare();
    const rows = await stmt.exec();
    stmt.destroy();

    expect(rows).toEqual(expected);
  });
});
