/* eslint-disable @typescript-eslint/no-explicit-any */
import {describe, test} from 'vitest';
import {createVitests} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';

const pgContent = await getChinook();
let _data: ReadonlyMap<string, readonly Row[]> | undefined;

describe(
  'Chinook PG Tests',
  {
    timeout: 30_000,
  },
  async () => {
    test.each(
      await createVitests(
        {
          suiteName: 'compiler_chinook',
          only: 'Flipped exists - or with normal exists',
          pgContent,
          zqlSchema: schema,
          setRawData: r => {
            _data = r;
          },
        },
        [
          {
            name: 'Flipped exists - simple',
            createQuery: b =>
              b.track.whereExists('album', a => a.where('title', 'Facelift'), {
                flip: true,
              }),
          },
          {
            name: 'Flipped exists - anded',
            createQuery: b =>
              b.album
                .whereExists('artist', a => a.where('name', 'Apocalyptica'), {
                  flip: true,
                })
                .whereExists('tracks', t => t.where('name', 'Sea Of Sorrow'), {
                  flip: true,
                }),
          },
          {
            name: "Flipped exists - or'ed",
            createQuery: b =>
              b.album.where(({or, exists}) =>
                or(
                  exists('artist', a => a.where('name', 'Apocalyptica'), {
                    flip: true,
                  }),
                  exists('artist', a => a.where('name', 'Fast As a Shark'), {
                    flip: true,
                  }),
                ),
              ),
          },
          {
            name: 'Flipped exists - or with normal exists',
            createQuery: b =>
              b.album.where(({or, exists}) =>
                or(
                  exists('artist', a => a.where('name', 'Apocalyptica'), {
                    flip: false,
                  }),
                  exists('artist', a => a.where('name', 'Fast As a Shark'), {
                    flip: true,
                  }),
                ),
              ),
          },
          // {
          //   name: 'Flipped exists - or with normal exists 2',
          //   createQuery: b =>
          //     b.album.where(({or, exists}) =>
          //       or(
          //         exists('artist', a => a.where('name', 'Apocalyptica'), {
          //           flip: true,
          //         }),
          //         exists('artist', a => a.where('name', 'Fast As a Shark'), {
          //           flip: false,
          //         }),
          //       ),
          //     ),
          // },
        ],
      ),
    )('$name', async ({fn}) => {
      await fn();
    });
  },
);
