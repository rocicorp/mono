import {describe, expect, test} from 'vitest';
import {JoinIndex} from './join-index.ts';

describe('unpartitioned', () => {
  test('add, lookup and remove', () => {
    const index = new JoinIndex(['orgID'], ['id']);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toBeUndefined();

    index.add({id: 'p1', orgID: 'a'});
    expect(index.size).toBe(1);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([undefined]);
    expect(index.lookup({orgID: 'b'}, ['orgID'])).toBeUndefined();

    index.add({id: 'p2', orgID: 'a'});
    index.add({id: 'p3', orgID: 'a'});
    expect(index.size).toBe(3);
    expect(index.entriesForTest()).toEqual({
      'j\x00sa\x00sp1': 1,
      'j\x00sa\x00sp2': 1,
      'j\x00sa\x00sp3': 1,
    });

    index.remove({id: 'p1', orgID: 'a'});
    index.remove({id: 'p3', orgID: 'a'});
    expect(index.size).toBe(1);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([undefined]);
    expect(index.entriesForTest()).toEqual({'j\x00sa\x00sp2': 1});

    index.remove({id: 'p2', orgID: 'a'});
    expect(index.size).toBe(0);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toBeUndefined();
    expect(index.entriesForTest()).toEqual({});
  });

  test('adding or removing a row twice is harmless', () => {
    const index = new JoinIndex(['orgID'], ['id']);
    index.add({id: 'p1', orgID: 'a'});
    index.add({id: 'p1', orgID: 'a'});
    index.add({id: 'p2', orgID: 'a'});
    index.add({id: 'p2', orgID: 'a'});
    expect(index.size).toBe(2);

    index.remove({id: 'p1', orgID: 'a'});
    index.remove({id: 'p1', orgID: 'a'});
    expect(index.size).toBe(1);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([undefined]);

    index.remove({id: 'p2', orgID: 'a'});
    index.remove({id: 'p2', orgID: 'a'});
    index.remove({id: 'p3', orgID: 'b'});
    expect(index.size).toBe(0);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toBeUndefined();
  });

  test('null join keys are neither indexed nor matched', () => {
    const index = new JoinIndex(['orgID'], ['id']);
    index.add({id: 'p1', orgID: null});
    expect(index.size).toBe(0);
    index.add({id: 'p2', orgID: 'a'});
    expect(index.lookup({orgID: null}, ['orgID'])).toBeUndefined();
    index.remove({id: 'p2', orgID: null});
    expect(index.size).toBe(1);
  });

  test('compound keys and values of different types', () => {
    const index = new JoinIndex(['a', 'b'], ['id']);
    index.add({id: 1, a: 1, b: 'x'});
    expect(index.lookup({ca: 1, cb: 'x'}, ['ca', 'cb'])).toEqual([undefined]);
    expect(index.lookup({ca: '1', cb: 'x'}, ['ca', 'cb'])).toBeUndefined();
    expect(index.lookup({ca: 1, cb: null}, ['ca', 'cb'])).toBeUndefined();
    expect(index.entriesForTest()).toEqual({'j\x00d1\x00sx\x00d1': 1});
  });
});

describe('partitioned', () => {
  test('lookup reports each partition that has the key', () => {
    const index = new JoinIndex(['orgID'], ['id'], ['region']);
    index.add({id: 'p1', orgID: 'a', region: 'east'});
    index.add({id: 'p2', orgID: 'a', region: 'east'});
    index.add({id: 'p3', orgID: 'a', region: 'west'});
    index.add({id: 'p4', orgID: 'b', region: 'west'});
    expect(index.size).toBe(4);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([
      {region: 'east'},
      {region: 'west'},
    ]);
    expect(index.entriesForTest()).toEqual({
      'j\x00sa\x00seast\x00sp1': 1,
      'j\x00sa\x00seast\x00sp2': 1,
      'j\x00sa\x00swest\x00sp3': 1,
      'j\x00sb\x00swest\x00sp4': 1,
    });

    index.remove({id: 'p1', orgID: 'a', region: 'east'});
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([
      {region: 'east'},
      {region: 'west'},
    ]);
    index.remove({id: 'p2', orgID: 'a', region: 'east'});
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([{region: 'west'}]);
    index.remove({id: 'p3', orgID: 'a', region: 'west'});
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toBeUndefined();
    expect(index.lookup({orgID: 'b'}, ['orgID'])).toEqual([{region: 'west'}]);
    expect(index.size).toBe(1);
  });

  test('partition values of different types are different partitions', () => {
    const index = new JoinIndex(['orgID'], ['id'], ['region']);
    index.add({id: 'p1', orgID: 'a', region: 1});
    index.add({id: 'p2', orgID: 'a', region: '1'});
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([
      {region: 1},
      {region: '1'},
    ]);
    index.remove({id: 'p2', orgID: 'a', region: 1});
    expect(index.size).toBe(2);
    index.remove({id: 'p1', orgID: 'a', region: 1});
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([{region: '1'}]);
  });

  test('compound partition key', () => {
    const index = new JoinIndex(['orgID'], ['id'], ['country', 'state']);
    index.add({id: 'p1', orgID: 'a', country: 'US', state: 'CA'});
    index.add({id: 'p1', orgID: 'a', country: 'US', state: 'CA'});
    expect(index.size).toBe(1);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toEqual([
      {country: 'US', state: 'CA'},
    ]);
    index.remove({id: 'p1', orgID: 'a', country: 'US', state: 'NY'});
    expect(index.size).toBe(1);
    index.remove({id: 'p1', orgID: 'a', country: 'US', state: 'CA'});
    expect(index.size).toBe(0);
    expect(index.lookup({orgID: 'a'}, ['orgID'])).toBeUndefined();
  });
});
