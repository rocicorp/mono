import {beforeEach, describe, expect, test} from 'vitest';
import {makeTestContext, TestContext} from '../context/test-context.js';
import type {Source} from '../ivm/source/source.js';
import {EntityQuery} from './entity-query.js';

describe('a limited window is correctly maintained over differences', () => {
  type E = {
    id: string;
  };
  const letters = 'abcdefghijklmnopqrstuvwxyz'.split('');

  let context: TestContext;
  let source: Source<E>;
  let q: EntityQuery<{e: E}>;
  beforeEach(() => {
    context = makeTestContext();
    source = context.getSource<E>('e');
    q = new EntityQuery<{e: E}>(context, 'e', 'e');
    Array.from({length: 10}, (_, i) => source.add({id: letters[i * 2 + 3]}));
  });

  test('adding values above the established window (asc)', async () => {
    const stmt = q.select('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    source.add({id: 'p'});
    const newData = await stmt.exec();

    // if we are limited and in ASC order, things above MAX are not added to the window
    expect(newData.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    stmt.destroy();
  });

  test('adding values below the established window (asc)', async () => {
    const stmt = q.select('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    source.add({id: 'c'});
    const newData = await stmt.exec();

    // if we are limited and in ASC order, things below MIN are added to the window
    expect(newData.map(x => x.id)).toEqual(['c', 'd', 'f', 'h', 'j']);

    stmt.destroy();
  });

  test('adding values above the established window (desc)', async () => {
    const stmt = q.select('id').desc('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    source.add({id: 'z'});
    const newData = await stmt.exec();

    // if we are limited and in DESC order, things above MAX are added to the window
    expect(newData.map(x => x.id)).toEqual(['z', 'v', 't', 'r', 'p']);

    stmt.destroy();
  });

  test('adding values below the established window (desc)', async () => {
    const stmt = q.select('id').desc('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    source.add({id: 'a'});
    const newData = await stmt.exec();

    // if we are limited and in DESC order, things below MIN are not added to the window
    expect(newData.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    stmt.destroy();
  });

  test('adding values inside the established window (asc)', async () => {
    const stmt = q.select('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    source.add({id: 'i'});
    const newData = await stmt.exec();

    // if we are limited and in ASC order, things inside the window are added
    expect(newData.map(x => x.id)).toEqual(['d', 'f', 'h', 'i', 'j']);

    stmt.destroy();
  });

  test('adding values inside the established window (desc)', async () => {
    const stmt = q.select('id').desc('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    source.add({id: 'q'});
    const newData = await stmt.exec();

    // if we are limited and in DESC order, things inside the window are added
    expect(newData.map(x => x.id)).toEqual(['v', 't', 'r', 'q', 'p']);

    stmt.destroy();
  });

  // This doesn't work yet. The window does not re-fill when data drops out.
  // The plan is to do the simple thing:
  // 1. over-fetch the window so we don't often under-run it
  // 2. when we do under-run, re-run the query from scratch
  // test('removing values inside the established window', async () => {
  //   const stmt = q.select('id').limit(5).prepare();
  //   const data = await stmt.exec();

  //   expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

  //   source.delete({id: 'h'});
  //   const newData = await stmt.exec();

  //   // if we are limited and in ASC order, things inside the window are removed
  //   expect(newData.map(x => x.id)).toEqual(['d', 'f', 'j', 'l', 'n']);

  //   stmt.destroy();
  // });
});
