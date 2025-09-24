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
          only: 'fuzz fail 1',
          pgContent,
          zqlSchema: schema,
          setRawData: r => {
            _data = r;
          },
          push: 0,
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
          {
            name: 'Flipped exists - or with normal exists 2',
            createQuery: b =>
              b.album.where(({or, exists}) =>
                or(
                  exists('artist', a => a.where('name', 'Apocalyptica'), {
                    flip: true,
                  }),
                  exists('artist', a => a.where('name', 'Fast As a Shark'), {
                    flip: false,
                  }),
                ),
              ),
          },
          {
            name: 'Flipped exists - or with other conditions',
            createQuery: b =>
              b.album.where(({or, cmp, exists}) =>
                or(
                  exists('artist', a => a.where('name', 'Apocalyptica'), {
                    flip: true,
                  }),
                  cmp('title', 'Black Sabbath'),
                  cmp('title', 'Chemical Wedding'),
                  cmp('title', 'Bongo Fury'),
                ),
              ),

            manualVerification: -[
              {
                artistId: 7,
                id: 9,
                title: 'Plays Metallica By Four Cellos',
              },
              {
                artistId: 12,
                id: 16,
                title: 'Black Sabbath',
              },
              {
                artistId: 14,
                id: 19,
                title: 'Chemical Wedding',
              },
              {
                artistId: 23,
                id: 31,
                title: 'Bongo Fury',
              },
            ],
          },
          {
            name: 'Flipped exists - in deeply nested logic',
            createQuery: b =>
              b.album.where(({or, and, exists}) =>
                or(
                  and(
                    exists('artist', a => a.where('name', 'Apocalyptica'), {
                      flip: true,
                    }),
                    exists('tracks', t => t.where('name', 'Enter Sandman'), {
                      flip: true,
                    }),
                  ),
                  and(
                    exists('artist', a => a.where('name', 'Audioslave'), {
                      flip: true,
                    }),
                    exists(
                      'tracks',
                      t => t.where('name', 'The Last Remaining Light'),
                      {flip: true},
                    ),
                  ),
                ),
              ),
            manualVerification: [
              {
                artistId: 7,
                id: 9,
                title: 'Plays Metallica By Four Cellos',
              },
              {
                artistId: 8,
                id: 10,
                title: 'Audioslave',
              },
            ],
          },
          {
            name: 'Flipped exists over junction edges',
            createQuery: b =>
              b.playlist.whereExists(
                'tracks',
                t => t.where('name', 'Enter Sandman'),
                {flip: true},
              ),
            manualVerification: [
              {
                id: 1,
                name: 'Music',
              },
              {
                id: 5,
                name: '90’s Music',
              },
              {
                id: 8,
                name: 'Music',
              },
              {
                id: 17,
                name: 'Heavy Metal Classic',
              },
            ],
          },
          {
            name: 'Flipped exists over junction edges w/ limit',
            createQuery: b =>
              b.playlist
                .whereExists('tracks', t => t.where('name', 'Enter Sandman'), {
                  flip: true,
                })
                .limit(1),
            manualVerification: [
              {
                id: 1,
                name: 'Music',
              },
            ],
          },
          {
            name: 'Flipped exists over junction edges w/ limit and alt sort',
            createQuery: b =>
              b.playlist
                .whereExists('tracks', t => t.where('name', 'Enter Sandman'), {
                  flip: true,
                })
                .limit(1)
                .orderBy('name', 'asc'),
            manualVerification: [
              {
                id: 5,
                name: '90’s Music',
              },
            ],
          },
          {
            name: 'fuzz fail 1',
            createQuery: b =>
              b.artist.whereExists('albums', q =>
                q.whereExists('artist', {flip: true}),
              ),
          },
        ],
      ),
    )('$name', async ({fn}) => {
      await fn();
    });
  },
);
