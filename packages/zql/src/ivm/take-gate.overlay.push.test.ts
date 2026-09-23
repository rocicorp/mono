import {expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {makeSourceChangeAdd} from './source.ts';
import {
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/fetch-and-push-tests.ts';
import type {Format} from './view.ts';

// A child change that adds several parents to a partitioned Take (a limit in a
// `related` subquery). Join pushes the parents one at a time. While Take
// handles the first one, it fetches its input, and the parents after it must
// not have the new child yet. TakeGate does not cap this parent fetch (its
// constraint has no partition key), so all of the parents get the push.
//
// Adding a1 pushes t1 then t2. For t1, Take evicts t4 and looks for the row
// before t4 to use as its new bound. That must be t3: if the fetch shows t2,
// Take takes t2 as its bound, drops the add of t2 as out of bounds, and later
// removes t2, which is not in the view.

const sources: Sources = {
  mediaType: {
    columns: {id: {type: 'string'}},
    primaryKeys: ['id'],
  },
  track: {
    columns: {
      id: {type: 'string'},
      albumID: {type: 'string'},
      mediaTypeID: {type: 'string'},
      ms: {type: 'number'},
    },
    primaryKeys: ['id'],
  },
  album: {
    columns: {id: {type: 'string'}},
    primaryKeys: ['id'],
  },
};

const sourceContents: SourceContents = {
  mediaType: [{id: 'm1'}],
  track: [
    {id: 't1', albumID: 'a1', mediaTypeID: 'm1', ms: 10},
    {id: 't2', albumID: 'a1', mediaTypeID: 'm1', ms: 35},
    {id: 't3', albumID: 'a2', mediaTypeID: 'm1', ms: 30},
    {id: 't4', albumID: 'a2', mediaTypeID: 'm1', ms: 40},
  ],
  album: [{id: 'a2'}],
};

function makeAST(flip: boolean): AST {
  return {
    table: 'mediaType',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['mediaTypeID']},
        subquery: {
          table: 'track',
          alias: 'tracks',
          orderBy: [
            ['ms', 'asc'],
            ['id', 'asc'],
          ],
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            flip,
            related: {
              system: 'client',
              correlation: {parentField: ['albumID'], childField: ['id']},
              subquery: {
                table: 'album',
                alias: 'zsubq_album',
                orderBy: [['id', 'asc']],
              },
            },
          },
          limit: 2,
        },
      },
    ],
  };
}

const format: Format = {
  singular: false,
  relationships: {
    tracks: {singular: false, relationships: {}},
  },
};

test.each([false, true])(
  'child add to several parents in a partitioned take (flip: %s)',
  flip => {
    const {data} = runPushTest({
      sources,
      sourceContents,
      ast: makeAST(flip),
      format,
      pushes: [
        ['album', makeSourceChangeAdd({id: 'a1'})],
        [
          'track',
          makeSourceChangeAdd({
            id: 't5',
            albumID: 'a2',
            mediaTypeID: 'm1',
            ms: 32,
          }),
        ],
      ],
    });

    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    const tracks = (data as any)[0].tracks.map((t: {id: string}) => t.id);
    expect(tracks).toEqual(['t1', 't3']);
  },
);
