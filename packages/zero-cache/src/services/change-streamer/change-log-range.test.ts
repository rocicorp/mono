import {describe, expect, test} from 'vitest';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import {foldIdentities} from './change-log-range.ts';

const table = (schema: string, name: string) => ({schema, name});

const rename = (
  from: [string, string],
  to: [string, string],
): SchemaChange => ({
  tag: 'rename-table',
  old: table(...from),
  new: table(...to),
});

const drop = (id: [string, string]): SchemaChange => ({
  tag: 'drop-table',
  id: table(...id),
});

describe('change-streamer/change-log-range', () => {
  describe('foldIdentities', () => {
    test('no renames or drops', () => {
      expect(foldIdentities([])).toEqual(new Map());
    });

    test('a rename maps the old identity to the new one', () => {
      expect(foldIdentities([rename(['my', 'foo'], ['my', 'bar'])])).toEqual(
        new Map([['my.foo', table('my', 'bar')]]),
      );
    });

    test('a second rename follows the first', () => {
      expect(
        foldIdentities([
          rename(['my', 'foo'], ['my', 'bar']),
          rename(['my', 'bar'], ['your', 'baz']),
        ]),
      ).toEqual(new Map([['my.foo', table('your', 'baz')]]));
    });

    test('a drop after a rename maps the original identity to null', () => {
      expect(
        foldIdentities([
          rename(['my', 'foo'], ['my', 'bar']),
          drop(['my', 'bar']),
        ]),
      ).toEqual(new Map([['my.foo', null]]));
    });

    test('unrelated tables are untouched', () => {
      expect(
        foldIdentities([
          rename(['my', 'foo'], ['my', 'bar']),
          drop(['my', 'other']),
        ]),
      ).toEqual(
        new Map([
          ['my.foo', table('my', 'bar')],
          ['my.other', null],
        ]),
      );
    });

    test('a completion does not drop the identity', () => {
      // A completion the subscriber has not followed is not its completion,
      // so its declaration must still resolve to a table.
      expect(
        foldIdentities([
          {
            tag: 'backfill-completed',
            relation: {schema: 'my', name: 'foo', rowKey: {columns: ['id']}},
            columns: ['a'],
            watermark: '0a',
          },
        ]),
      ).toEqual(new Map());
    });

    test('a name reused by a different table is not confused with the first', () => {
      // `foo` is renamed away, and a new `foo` is created and then renamed.
      expect(
        foldIdentities([
          rename(['my', 'foo'], ['my', 'old_foo']),
          rename(['my', 'foo'], ['my', 'new_foo']),
        ]),
        // Only the first is tracked: a declaration names a table the
        // subscriber knew at its watermark, and the second `my.foo` did not
        // exist then.
      ).toEqual(new Map([['my.foo', table('my', 'old_foo')]]));
    });
  });
});
