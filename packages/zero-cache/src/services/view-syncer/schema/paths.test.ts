import {compareUTF8} from 'compare-utf8';
import {describe, expect, test} from 'vitest';
import {CVRPaths, lastActiveIndex} from './paths.js';
import {oneAfter, type CVRVersion, type RowID} from './types.js';

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
    ).toBe('/vs/cvr/456/p/m/2abc:0z/q/boo-query');
    expect(
      paths2.queryPatch(
        {stateVersion: '2abc', minorVersion: 36},
        {id: 'boo-query'},
      ),
    ).toBe('/vs/cvr/456/p/m/2abc:110/q/boo-query');
  });

  test('version from patch path', () => {
    const paths = new CVRPaths('fbr');
    (
      [
        [
          '/vs/cvr/fbr/p/d/28c8:12s/r/PNJVDvpmnF-qcv1Mw8AfiQ',
          {stateVersion: '28c8', minorVersion: 100},
        ],
        [
          '/vs/cvr/fbr/p/d/28c8/r/PNJVDvpmnF-qcv1Mw8AfiQ',
          {stateVersion: '28c8'},
        ],
        ['/vs/cvr/fbr/p/m/01/c/foo', {stateVersion: '01'}],
        ['/vs/cvr/fbr/p/m/01:02/c/foo', {stateVersion: '01', minorVersion: 2}],
      ] satisfies [path: string, ver: CVRVersion][]
    ).forEach(c => {
      expect(paths.versionFromPatchPath(c[0])).toEqual(c[1]);
    });
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
    ).toBe('/vs/cvr/fbr/p/d/28c8:12s/r/PNJVDvpmnF-qcv1Mw8AfiQ');
  });

  test('row patch prefixes', () => {
    const paths = new CVRPaths('fbr');

    const v1: CVRVersion = {stateVersion: '1ab'};
    const v2: CVRVersion = {stateVersion: '1ab', minorVersion: 1};
    const v3: CVRVersion = {stateVersion: '1ac'};
    const row: RowID = {
      schema: 'public',
      table: 'issues',
      rowKey: {id: 123},
    };

    expect(paths.rowPatchVersionPrefix(v1)).toBe('/vs/cvr/fbr/p/d/1ab/');
    expect(paths.rowPatchVersionPrefix(v2)).toBe('/vs/cvr/fbr/p/d/1ab:01/');
    expect(paths.rowPatchVersionPrefix(v3)).toBe('/vs/cvr/fbr/p/d/1ac/');

    const ordered = [
      paths.rowPatchVersionPrefix(v1),
      paths.rowPatch(v1, row),
      paths.rowPatchVersionPrefix(v2),
      paths.rowPatch(v2, row),
      paths.rowPatchVersionPrefix(oneAfter(v2)),
      paths.rowPatchVersionPrefix(v3),
      paths.rowPatch(v3, row),
      paths.rowPatchVersionPrefix(oneAfter(v3)),
    ];
    for (let i = 0; i < ordered.length; i++) {
      for (let j = i + 1; j < ordered.length; j++) {
        expect(compareUTF8(ordered[i], ordered[j])).toBeLessThan(0);
        expect(compareUTF8(ordered[j], ordered[i])).toBeGreaterThan(0);
      }
    }
  });

  test('metadata patch prefixes', () => {
    const paths = new CVRPaths('fbr');

    const v1: CVRVersion = {stateVersion: '1ab'};
    const v2: CVRVersion = {stateVersion: '1ab', minorVersion: 1};
    const v3: CVRVersion = {stateVersion: '1ac'};

    expect(paths.metadataPatchPrefix()).toBe('/vs/cvr/fbr/p/m/');
    expect(paths.metadataPatchVersionPrefix(v1)).toBe('/vs/cvr/fbr/p/m/1ab/');
    expect(paths.metadataPatchVersionPrefix(v2)).toBe(
      '/vs/cvr/fbr/p/m/1ab:01/',
    );
    expect(paths.metadataPatchVersionPrefix(v3)).toBe('/vs/cvr/fbr/p/m/1ac/');

    const ordered = [
      paths.metadataPatchVersionPrefix(v1),
      paths.clientPatch(v1, {id: 'foo'}),
      paths.metadataPatchVersionPrefix(v2),
      paths.clientPatch(v2, {id: 'foo'}),
      paths.metadataPatchVersionPrefix(oneAfter(v2)),
      paths.metadataPatchVersionPrefix(v3),
      paths.queryPatch(v3, {id: 'foo'}),
      paths.metadataPatchVersionPrefix(oneAfter(v3)),
    ];
    for (let i = 0; i < ordered.length; i++) {
      for (let j = i + 1; j < ordered.length; j++) {
        expect(compareUTF8(ordered[i], ordered[j])).toBeLessThan(0);
        expect(compareUTF8(ordered[j], ordered[i])).toBeGreaterThan(0);
      }
    }
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
