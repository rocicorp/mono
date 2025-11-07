import {beforeEach, describe, expect, test} from 'vitest';
import {MemoryStorage} from './memory-storage.ts';
import {KeySet} from './join-utils.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';

describe('KeySet', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  describe('with valueKey', () => {
    let keySet: KeySet<CompoundKey>;
    const setName = 'userPosts';
    const setKey: CompoundKey = ['userId'];
    const primaryKey: CompoundKey = ['postId'];
    const valueKey: CompoundKey = ['postTitle', 'category'];

    // Test data
    const row1: Row = {
      userId: 'u1',
      postId: 'p1',
      postTitle: 'Hello',
      category: 'Tech',
    };
    const row2: Row = {
      userId: 'u1',
      postId: 'p2',
      postTitle: 'World',
      category: 'Life',
    };
    const row3: Row = {
      userId: 'u2',
      postId: 'p3',
      postTitle: 'Other',
      category: 'Misc',
    };
    const row4: Row = {
      userId: 'u1',
      postId: 'p4',
      postTitle: 'Hello',
      category: 'Tech',
    };

    beforeEach(() => {
      keySet = new KeySet(storage, setName, setKey, primaryKey, valueKey);
    });

    test('should add a row by constructing the correct key', () => {
      keySet.add(row1);
      expect(storage.cloneData()).toMatchInlineSnapshot(`
        {
          ""userPosts","[\\"u1\\"]","[\\"Hello\\",\\"Tech\\"]","[\\"p1\\"]",": true,
        }
      `);
    });

    test('should delete a row by constructing the correct key', () => {
      keySet.add(row1);
      expect(storage.cloneData()).toMatchInlineSnapshot(`
        {
          ""userPosts","[\\"u1\\"]","[\\"Hello\\",\\"Tech\\"]","[\\"p1\\"]",": true,
        }
      `);

      keySet.delete(row1);
      expect(storage.cloneData()).toMatchInlineSnapshot(`{}`);
    });

    test('should correctly report if a set is empty', () => {
      // Check set "u1"
      expect(keySet.isEmpty({userId: 'u1'})).toBe(true);
      keySet.add(row1);
      expect(keySet.isEmpty({userId: 'u1'})).toBe(false);

      // Check set "u2" (should still be empty)
      expect(keySet.isEmpty({userId: 'u2'})).toBe(true);
      keySet.add(row3);
      expect(keySet.isEmpty({userId: 'u2'})).toBe(false);

      // Deleting row 1 should not empty set "u1" if row 2 is present
      keySet.add(row2);
      keySet.delete(row1);
      expect(keySet.isEmpty({userId: 'u1'})).toBe(false);

      // Deleting row 2 should now empty set "u1"
      keySet.delete(row2);
      expect(keySet.isEmpty({userId: 'u1'})).toBe(true);
    });

    test('should get unique values for a set, respecting deduplication', () => {
      keySet.add(row1); // "u1" -> { Hello, Tech } (via p1)
      keySet.add(row2); // "u1" -> { World, Life } (via p2)
      keySet.add(row3); // "u2" -> { Other, Misc } (via p3)
      keySet.add(row4); // "u1" -> { Hello, Tech } (via p4)

      // Check the results. Should be sorted by key, then deduplicated.
      // key1 and key4 have the same value, should only appear once.
      // key2 has a different value.
      expect(Array.from(keySet.getValues({userId: 'u1'}))).toEqual([
        {postTitle: 'Hello', category: 'Tech'},
        {postTitle: 'World', category: 'Life'},
      ]);
      expect(Array.from(keySet.getValues({userId: 'u2'}))).toEqual([
        {postTitle: 'Other', category: 'Misc'},
      ]);
      expect(Array.from(keySet.getValues({userId: 'u3'}))).toEqual([]);

      keySet.delete(row4);
      expect(Array.from(keySet.getValues({userId: 'u1'}))).toEqual([
        {postTitle: 'Hello', category: 'Tech'},
        {postTitle: 'World', category: 'Life'},
      ]);
      expect(Array.from(keySet.getValues({userId: 'u2'}))).toEqual([
        {postTitle: 'Other', category: 'Misc'},
      ]);
      expect(Array.from(keySet.getValues({userId: 'u3'}))).toEqual([]);

      keySet.delete(row3);
      expect(Array.from(keySet.getValues({userId: 'u1'}))).toEqual([
        {postTitle: 'Hello', category: 'Tech'},
        {postTitle: 'World', category: 'Life'},
      ]);
      expect(Array.from(keySet.getValues({userId: 'u2'}))).toEqual([]);
      expect(Array.from(keySet.getValues({userId: 'u3'}))).toEqual([]);

      keySet.delete(row2);
      expect(Array.from(keySet.getValues({userId: 'u1'}))).toEqual([
        {postTitle: 'Hello', category: 'Tech'},
      ]);
      expect(Array.from(keySet.getValues({userId: 'u2'}))).toEqual([]);
      expect(Array.from(keySet.getValues({userId: 'u3'}))).toEqual([]);

      keySet.delete(row1);
      expect(Array.from(keySet.getValues({userId: 'u1'}))).toEqual([]);
      expect(Array.from(keySet.getValues({userId: 'u2'}))).toEqual([]);
      expect(Array.from(keySet.getValues({userId: 'u3'}))).toEqual([]);
    });
  });

  describe('without valueKey (undefined)', () => {
    let keySet: KeySet<undefined>;
    const setName = 'userSet';
    const setKey: CompoundKey = ['userId'];
    const primaryKey: CompoundKey = ['itemId'];
    const valueKey = undefined;

    const row1: Row = {userId: 'u1', itemId: 'i1'};
    const row2: Row = {userId: 'u1', itemId: 'i2'};
    const row3: Row = {userId: 'u2', itemId: 'i3'};

    beforeEach(() => {
      keySet = new KeySet(storage, setName, setKey, primaryKey, valueKey);
    });

    test('should add a row with the correct (simpler) key', () => {
      keySet.add(row1);
      expect(storage.cloneData()).toMatchInlineSnapshot(`
        {
          ""userSet","[\\"u1\\"]","[\\"i1\\"]",": true,
        }
      `);
    });

    test('should delete a row with the correct (simpler) key', () => {
      keySet.add(row1);
      expect(storage.cloneData()).toMatchInlineSnapshot(`
        {
          ""userSet","[\\"u1\\"]","[\\"i1\\"]",": true,
        }
      `);

      keySet.delete(row1);
      expect(storage.cloneData()).toMatchInlineSnapshot(`{}`);
    });

    test('should correctly report if a set is empty', () => {
      expect(keySet.isEmpty({userId: 'u1'})).toBe(true);
      keySet.add(row1);
      expect(keySet.isEmpty({userId: 'u1'})).toBe(false);

      keySet.add(row2);
      keySet.delete(row1);
      expect(keySet.isEmpty({userId: 'u1'})).toBe(false); // row2 still exists

      keySet.delete(row2);
      expect(keySet.isEmpty({userId: 'u1'})).toBe(true);
    });

    test('should return an empty iterable from getValues', () => {
      keySet.add(row1);
      keySet.add(row2);
      keySet.add(row3);

      const values = Array.from(keySet.getValues({userId: 'u1'}));
      expect(values).toEqual([]);
    });
  });
});
