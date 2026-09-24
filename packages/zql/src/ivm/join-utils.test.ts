import {describe, expect, test} from 'vitest';
import {
  buildJoinConstraint,
  decodePartitionConstraint,
  getMatchingParentEntries,
  indexParentInStorage,
  isJoinMatch,
  makePartitionStorageKey,
  makeUnpartitionedStorageKey,
  rowEqualsForCompoundKey,
  splitPartitionAndPk,
  unindexParentInStorage,
} from './join-utils.ts';
import {MemoryStorage} from './memory-storage.ts';

describe('rowEqualsForCompoundKey', () => {
  test('single key match', () => {
    expect(rowEqualsForCompoundKey({id: 1}, {id: 1}, ['id'])).toBe(true);
  });

  test('single key mismatch', () => {
    expect(rowEqualsForCompoundKey({id: 1}, {id: 2}, ['id'])).toBe(false);
  });

  test('compound key all match', () => {
    expect(
      rowEqualsForCompoundKey({a: 1, b: 'x'}, {a: 1, b: 'x'}, ['a', 'b']),
    ).toBe(true);
  });

  test('compound key partial mismatch', () => {
    expect(
      rowEqualsForCompoundKey({a: 1, b: 'x'}, {a: 1, b: 'y'}, ['a', 'b']),
    ).toBe(false);
  });

  test('null equals null (compareValues treats null as a real value)', () => {
    expect(rowEqualsForCompoundKey({id: null}, {id: null}, ['id'])).toBe(true);
  });

  test('extra columns ignored', () => {
    expect(
      rowEqualsForCompoundKey({id: 1, val: 'a'}, {id: 1, val: 'b'}, ['id']),
    ).toBe(true);
  });
});

describe('isJoinMatch', () => {
  test('single key match', () => {
    expect(isJoinMatch({id: 1}, ['id'], {id: 1}, ['id'])).toBe(true);
  });

  test('single key mismatch', () => {
    expect(isJoinMatch({id: 1}, ['id'], {id: 2}, ['id'])).toBe(false);
  });

  test('compound key match with different column names', () => {
    expect(
      isJoinMatch({a: 1, b: 'x'}, ['a', 'b'], {x: 1, y: 'x'}, ['x', 'y']),
    ).toBe(true);
  });

  test('null parent value returns false (SQL NULL semantics)', () => {
    expect(isJoinMatch({id: null}, ['id'], {id: 1}, ['id'])).toBe(false);
  });

  test('null child value returns false', () => {
    expect(isJoinMatch({id: 1}, ['id'], {id: null}, ['id'])).toBe(false);
  });

  test('both null returns false (unlike rowEqualsForCompoundKey)', () => {
    expect(isJoinMatch({id: null}, ['id'], {id: null}, ['id'])).toBe(false);
  });
});

describe('buildJoinConstraint', () => {
  test('single key maps value correctly', () => {
    expect(buildJoinConstraint({id: 1}, ['id'], ['id'])).toEqual({id: 1});
  });

  test('compound key maps all values', () => {
    expect(buildJoinConstraint({a: 1, b: 'x'}, ['a', 'b'], ['a', 'b'])).toEqual(
      {a: 1, b: 'x'},
    );
  });

  test('null value returns undefined', () => {
    expect(buildJoinConstraint({id: null}, ['id'], ['id'])).toBeUndefined();
  });

  test('null in second position returns undefined', () => {
    expect(
      buildJoinConstraint({a: 1, b: null}, ['a', 'b'], ['x', 'y']),
    ).toBeUndefined();
  });

  test('different source/target key names', () => {
    expect(
      buildJoinConstraint(
        {userId: 5, orgId: 10},
        ['userId', 'orgId'],
        ['id', 'org'],
      ),
    ).toEqual({id: 5, org: 10});
  });
});

describe('splitPartitionAndPk', () => {
  test('single partition key and single pk', () => {
    expect(splitPartitionAndPk('ss0\x00si0', 1)).toEqual(['ss0', 'si0']);
  });

  test('compound partition key and single pk', () => {
    expect(splitPartitionAndPk('sUS\x00sCA\x00si0', 2)).toEqual([
      'sUS\x00sCA',
      'si0',
    ]);
  });

  test('compound partition key and compound pk', () => {
    expect(splitPartitionAndPk('sUS\x00sCA\x00sTenant1\x00si0', 2)).toEqual([
      'sUS\x00sCA',
      'sTenant1\x00si0',
    ]);
  });
});

describe('join storage key formatting and decoding', () => {
  test('makeUnpartitionedStorageKey', () => {
    expect(makeUnpartitionedStorageKey('sjoin', 'spk')).toBe(
      'j\x00sjoin\x00spk',
    );
  });

  test('makePartitionStorageKey', () => {
    expect(makePartitionStorageKey('sjoin', 'spart', 'spk')).toBe(
      'j\x00sjoin\x00spart\x00spk',
    );
  });

  test('decodePartitionConstraint single and compound', () => {
    expect(decodePartitionConstraint('sUS', ['country'])).toEqual({
      country: 'US',
    });
    expect(
      decodePartitionConstraint('sUS\x00sCA', ['country', 'state']),
    ).toEqual({
      country: 'US',
      state: 'CA',
    });
  });
});

describe('join storage index and matching', () => {
  test('unpartitioned storage key and operations', () => {
    const storage = new MemoryStorage();
    const parent = {id: 'p1', orgId: 'orgA'};
    indexParentInStorage(storage, parent, ['orgId'], ['id']);

    expect(storage.cloneData()).toEqual({
      'j\x00sorgA\x00sp1': 1,
    });

    const matching = getMatchingParentEntries(storage, {orgId: 'orgA'}, [
      'orgId',
    ]);
    expect(matching).toBeDefined();
    expect(matching?.length).toBe(1);
    expect(matching?.[0].pks).toEqual(new Set(['sp1']));

    // Second parent with same join key
    indexParentInStorage(storage, {id: 'p2', orgId: 'orgA'}, ['orgId'], ['id']);
    const matching2 = getMatchingParentEntries(storage, {orgId: 'orgA'}, [
      'orgId',
    ]);
    expect(matching2?.[0].pks).toEqual(new Set(['sp1', 'sp2']));

    // Unindex one parent
    unindexParentInStorage(storage, parent, ['orgId'], ['id']);
    const matching3 = getMatchingParentEntries(storage, {orgId: 'orgA'}, [
      'orgId',
    ]);
    expect(matching3?.[0].pks).toEqual(new Set(['sp2']));

    // Unindex second parent
    unindexParentInStorage(
      storage,
      {id: 'p2', orgId: 'orgA'},
      ['orgId'],
      ['id'],
    );
    expect(storage.cloneData()).toEqual({});
    expect(
      getMatchingParentEntries(storage, {orgId: 'orgA'}, ['orgId']),
    ).toBeUndefined();
  });

  test('partitioned storage key and operations', () => {
    const storage = new MemoryStorage();
    const p1 = {id: 'p1', orgId: 'orgA', region: 'east'};
    const p2 = {id: 'p2', orgId: 'orgA', region: 'east'};
    const p3 = {id: 'p3', orgId: 'orgA', region: 'west'};

    indexParentInStorage(storage, p1, ['orgId'], ['id'], ['region']);
    indexParentInStorage(storage, p2, ['orgId'], ['id'], ['region']);
    indexParentInStorage(storage, p3, ['orgId'], ['id'], ['region']);

    expect(storage.cloneData()).toEqual({
      'j\x00sorgA\x00seast\x00sp1': 1,
      'j\x00sorgA\x00seast\x00sp2': 1,
      'j\x00sorgA\x00swest\x00sp3': 1,
    });

    const matching = getMatchingParentEntries(
      storage,
      {orgId: 'orgA'},
      ['orgId'],
      ['region'],
    );
    expect(matching).toBeDefined();
    expect(matching?.length).toBe(2);

    expect(matching?.[0]).toEqual({
      pks: new Set(['sp1', 'sp2']),
      partitionConstraint: {region: 'east'},
    });
    expect(matching?.[1]).toEqual({
      pks: new Set(['sp3']),
      partitionConstraint: {region: 'west'},
    });

    // Unindex p1
    unindexParentInStorage(storage, p1, ['orgId'], ['id'], ['region']);
    const matchingAfter = getMatchingParentEntries(
      storage,
      {orgId: 'orgA'},
      ['orgId'],
      ['region'],
    );
    expect(matchingAfter?.[0].pks).toEqual(new Set(['sp2']));
  });
});
