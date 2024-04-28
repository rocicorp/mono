import {describe, expect, test} from 'vitest';
import {CVRPaths, lastActiveIndex} from './paths.js';

describe('view-syncer/schema/paths', () => {
  test('patch path versioning', () => {
    const paths1 = new CVRPaths('123');
    expect(paths1.queryPatch({stateVersion: '1zb'}, {id: 'foo-query'})).toBe(
      '/vs/cvr/123/p/m/1zb/q/foo-query',
    );
    expect(
      paths1.queryPatch(
        {stateVersion: '1zb', minorVersion: 0},
        {id: 'foo-query'},
      ),
    ).toBe('/vs/cvr/123/p/m/1zb/q/foo-query');

    const paths2 = new CVRPaths('456');
    expect(
      paths2.queryPatch(
        {stateVersion: '2abc', minorVersion: 35},
        {id: 'boo-query'},
      ),
    ).toBe('/vs/cvr/456/p/m/2abc.0z/q/boo-query');
    expect(
      paths2.queryPatch(
        {stateVersion: '2abc', minorVersion: 36},
        {id: 'boo-query'},
      ),
    ).toBe('/vs/cvr/456/p/m/2abc.110/q/boo-query');
  });

  test('client paths', () => {
    const paths = new CVRPaths('abc');

    expect(paths.client({id: 'foo'})).toBe('/vs/cvr/abc/m/c/foo');
    expect(paths.clientPatch({stateVersion: '2321'}, {id: 'foo'})).toBe(
      '/vs/cvr/abc/p/m/2321/c/foo',
    );
  });

  test('row paths', () => {
    const paths = new CVRPaths('fbr');
    expect(
      paths.row({
        schema: 'public',
        table: 'issues',
        rowKey: {id: 123},
      }),
    ).toBe('/vs/cvr/fbr/d/r/hmiZ0jkPKW203clzP4Mg6w');
    expect(
      paths.row({
        schema: 'public',
        table: 'issues',
        rowKey: {id: 124},
      }),
    ).toBe('/vs/cvr/fbr/d/r/Z1Lzg3qVqQAbTf_PsUvlCg');
    expect(
      paths.row({
        schema: 'public',
        table: 'issues',
        rowKey: {
          this: `could be a really a big row k${'e'.repeat(1000)}y`,
        },
      }),
    ).toBe('/vs/cvr/fbr/d/r/PNJVDvpmnF-qcv1Mw8AfiQ');

    expect(
      paths.rowPatch(
        {stateVersion: '28c8', minorVersion: 100},
        {
          schema: 'public',
          table: 'issues',
          rowKey: {
            this: `could be a really a big row k${'e'.repeat(1000)}y`,
          },
        },
      ),
    ).toBe('/vs/cvr/fbr/p/d/28c8.12s/r/PNJVDvpmnF-qcv1Mw8AfiQ');
  });

  test('last active paths', () => {
    expect(lastActiveIndex.dayPrefix(Date.UTC(2024, 3, 19, 1, 2, 3))).toBe(
      '/vs/lastActive/2024-04-19',
    );
    expect(lastActiveIndex.dayPrefix(Date.UTC(2024, 3, 19, 3, 2, 1))).toBe(
      '/vs/lastActive/2024-04-19',
    );
    expect(
      lastActiveIndex.entry('foo-cvr', Date.UTC(2024, 2, 28, 3, 48, 29)),
    ).toBe('/vs/lastActive/2024-03-28/foo-cvr');
  });
});
