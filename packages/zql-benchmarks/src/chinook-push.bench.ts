import {runBenchmarks} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import type {PullRow} from '../../zql/src/query/query.ts';

const pgContent = await getChinook();

function defaultTrack(id: number): PullRow<'track', typeof schema> {
  return {
    id,
    name: `Track ${id}`,
    albumId: 248,
    mediaTypeId: 1,
    genreId: 1,
    composer: 'Composer',
    milliseconds: 1000,
    bytes: 1000,
    unitPrice: 1.99,
  };
}

await runBenchmarks(
  {
    suiteName: 'chinook_bench',
    type: 'push',
    pgContent,
    zqlSchema: schema,
  },
  [
    {
      name: 'push into unlimited query',
      createQuery: q => q.track,
      generatePush: i => [
        [
          'track',
          {
            type: 'add',
            row: defaultTrack(i + 10_000),
          },
        ],
      ],
    },
    {
      name: 'push into limited query, outside the bound',
      createQuery: q => q.track.limit(100),
      generatePush: i => [
        [
          'track',
          {
            type: 'add',
            row: defaultTrack(i + 10_000),
          },
        ],
      ],
    },
    {
      name: 'push into limited query, inside the bound',
      createQuery: q => q.track.limit(100),
      generatePush: i => [
        [
          'track',
          {
            type: 'add',
            row: defaultTrack(-1 * i),
          },
        ],
      ],
    },
    // For some reason tinybench is not respecting limits on number of
    // iterations and warmups to run. It is blowing past the track table size
    // and removing all rows during test warmup.
    // We should switch to https://github.com/evanwashere/mitata as
    // tinybench also has bugs in `setup` and `teardown`: https://github.com/rocicorp/mono/pull/4313
    // {
    //   name: 'remove from limited query, outside the bound',
    //   createQuery: q => q.track.limit(100),
    //   generatePush: i => [
    //     [
    //       'track',
    //       {
    //         type: 'remove',
    //         row: defaultTrack(i + 1_000),
    //       },
    //     ],
    //   ],
    // },
    // {
    //   name: 'remove from limited query, inside the bound',
    //   createQuery: q => q.track.limit(100),
    //   generatePush: i => [
    //     [
    //       'track',
    //       {
    //         type: 'remove',
    //         row: defaultTrack(i + 1),
    //       },
    //     ],
    //   ],
    // },
  ],
);
