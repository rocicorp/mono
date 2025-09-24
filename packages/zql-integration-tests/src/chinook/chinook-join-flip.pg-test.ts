/* eslint-disable @typescript-eslint/no-explicit-any */
import {describe, test} from 'vitest';
import {createVitests} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const pgContent = await getChinook();

describe(
  'Chinook PG Tests',
  {
    timeout: 30_000,
  },
  async () => {
    test.each(
      await createVitests(
        {
          suiteName: 'chinook_join_flip',
          pgContent,
          zqlSchema: schema,
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

            manualVerification: [
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
            name: 'Flipped exists - anded again (1-1 and 1-many)',
            createQuery: b =>
              b.album.where(({and, exists}) =>
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
          },
          {
            name: 'Flipped exists - deeply nested logic',
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
            name: 'case where the fetch constraint will not match the parent constraint',
            createQuery: b =>
              b.artist
                .whereExists('albums', q =>
                  q.whereExists('artist', {flip: true}),
                )
                .limit(25),
          },
          {
            name: 'Flipped exists - anded, with non-flipped',
            createQuery: b =>
              b.album
                .whereExists('artist', a => a.where('name', 'Apocalyptica'), {
                  flip: true,
                })
                .whereExists('tracks', t => t.where('name', 'Sea Of Sorrow'), {
                  flip: false,
                }),
          },
          {
            name: 'Flipped exists - in deeply nested logic combined with non-flipped',
            createQuery: b =>
              b.album.where(({or, and, exists}) =>
                or(
                  and(
                    exists('artist', a => a.where('name', 'Apocalyptica'), {
                      flip: true,
                    }),
                    exists('tracks', t => t.where('name', 'Enter Sandman'), {
                      flip: false,
                    }),
                  ),
                  and(
                    exists('artist', a => a.where('name', 'Audioslave'), {
                      flip: true,
                    }),
                    exists(
                      'tracks',
                      t => t.where('name', 'The Last Remaining Light'),
                      {flip: false},
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
        ],
      ),
    )('$name', async ({fn}) => {
      await fn();
    });
  },
);

/*
- child.fetch: this is the pre-flip child.
This is `artist` in this case.

OK.

We get the artists -- all artists.

Then for each artist we get the parents.

We should fail to get some parents...
*/
