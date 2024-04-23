import {describe, expect, test} from 'vitest';
import {rowKeyHash} from '../../../types/row-key.js';
import {CVRPaths, LastActiveIndex} from './paths.js';

describe('view-syncer/schema/paths', () => {
  test('patch path versioning', () => {
    const paths1 = new CVRPaths('123');
    expect(paths1.queryPatch({stateVersion: '1zb'}, {id: 'foo-query'})).toBe(
      '/vs/cvr/123/patches/1zb/queries/foo-query',
    );

    const paths2 = new CVRPaths('456');
    expect(
      paths2.queryPatch(
        {stateVersion: '2abc', metaVersion: 35},
        {id: 'boo-query'},
      ),
    ).toBe('/vs/cvr/456/patches/2abc-0z/queries/boo-query');
    expect(
      paths2.queryPatch(
        {stateVersion: '2abc', metaVersion: 36},
        {id: 'boo-query'},
      ),
    ).toBe('/vs/cvr/456/patches/2abc-110/queries/boo-query');
  });

  test('client paths', () => {
    const paths = new CVRPaths('abc');

    expect(paths.client({id: 'foo'})).toBe('/vs/cvr/abc/meta/clients/foo');
    expect(paths.clientPatch({stateVersion: '2321'}, {id: 'foo'})).toBe(
      '/vs/cvr/abc/patches/2321/clients/foo',
    );
  });

  test('row paths', () => {
    const paths = new CVRPaths('fbr');
    expect(
      paths.row({
        schema: 'public',
        table: 'issues',
        rowKeyHash: rowKeyHash({id: 123}),
      }),
    ).toBe('/vs/cvr/fbr/rows/public/issues/qse5G7quj_el4_x5CbWzQg');
    expect(
      paths.row({
        schema: 'public',
        table: 'issues',
        rowKeyHash: rowKeyHash({id: 124}),
      }),
    ).toBe('/vs/cvr/fbr/rows/public/issues/u1Ny9isI-KQXSET6KchNbw');
    expect(
      paths.row({
        schema: 'schema/with/slashes',
        table: 'table"with"double"quotes',
        rowKeyHash: rowKeyHash({id: 120}),
      }),
    ).toBe(
      '/vs/cvr/fbr/rows/"schema/with/slashes"/"table\\"with\\"double\\"quotes"/SzkDbDPMPjYy-AcfV3DrQA',
    );
  });

  test('last active paths', () => {
    const paths = new LastActiveIndex();

    expect(paths.dayPrefix(new Date(Date.UTC(2024, 3, 19, 1, 2, 3)))).toBe(
      '/vs/lastActive/2024-04-19',
    );
    expect(paths.dayPrefix(new Date(Date.UTC(2024, 3, 19, 3, 2, 1)))).toBe(
      '/vs/lastActive/2024-04-19',
    );
    expect(
      paths.entry('foo-cvr', new Date(Date.UTC(2024, 2, 28, 3, 48, 29))),
    ).toBe('/vs/lastActive/2024-03-28/foo-cvr');
  });
});
