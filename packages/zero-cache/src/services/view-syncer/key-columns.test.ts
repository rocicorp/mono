import {describe, expect, test} from 'vitest';
import {KeyColumns} from './key-columns.ts';
import type {RowID, RowRecord} from './schema/types.ts';

describe('key columns', () => {
  function rowRecord(rowID: RowID): RowRecord {
    return {
      id: rowID,
      refCounts: null,
      rowVersion: '01',
      patchVersion: {stateVersion: '01'},
    };
  }

  const cvrRows: RowRecord[] = [
    rowRecord({
      schema: 'public',
      table: 'user',
      rowKey: {id: 'foo'},
    }),
    rowRecord({
      schema: 'public',
      table: 'user',
      rowKey: {id: 'bar'},
    }),
    rowRecord({
      schema: 'public',
      table: 'issueLabel',
      rowKey: {issueID: 'bar', labelID: 'rab'},
    }),
  ];

  test('no change in key', () => {
    const keyColumns = new KeyColumns(cvrRows);
    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'user',
          rowKey: {id: 'boo'},
        },
        {id: 'boo', value: 'zoo'},
      ),
    ).toBeNull();

    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'user',
          rowKey: {id: 'doo'},
        },
        {id: 'doo', value: 'foo'},
      ),
    ).toBeNull();

    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'issueLabel',
          rowKey: {labelID: 'order', issueID: 'agnostic'},
        },
        {labelID: 'order', issueID: 'agnostic', value: 'foo'},
      ),
    ).toBeNull();
  });

  test('no rows for table', () => {
    const keyColumns = new KeyColumns(cvrRows);
    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'emoji',
          rowKey: {id: 'roo'},
        },
        {id: 'roo', value: 'woo'},
      ),
    ).toBeNull();
  });

  test('expanded key', () => {
    const keyColumns = new KeyColumns(cvrRows);
    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'user',
          rowKey: {id: 'boo', username: 'blue'},
        },
        {id: 'boo', username: 'blue', value: 'zoo'},
      ),
    ).toEqual({
      schema: 'public',
      table: 'user',
      rowKey: {id: 'boo'},
    });

    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'user',
          rowKey: {id: 'doo', username: 'oof'},
        },
        {id: 'doo', username: 'oof', value: 'foo'},
      ),
    ).toEqual({
      schema: 'public',
      table: 'user',
      rowKey: {id: 'doo'},
    });
  });

  test('completely different key', () => {
    const keyColumns = new KeyColumns(cvrRows);
    expect(
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'issueLabel',
          rowKey: {id: 'single-key'},
        },
        {id: 'single-key', issueID: 'legacy', labelID: 'key'},
      ),
    ).toEqual({
      schema: 'public',
      table: 'issueLabel',
      rowKey: {issueID: 'legacy', labelID: 'key'},
    });
  });

  test('old key with non-existent column', () => {
    const keyColumns = new KeyColumns(cvrRows);
    expect(() =>
      keyColumns.getOldRowID(
        {
          schema: 'public',
          table: 'issueLabel',
          rowKey: {id: 'single-key'},
        },
        {id: 'single-key', no: 'compound', key: 'here'},
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"ClientNotFound","message":"CVR contains key column \\"issueID\\" that is no longer in the replica"}]`,
    );
  });
});
