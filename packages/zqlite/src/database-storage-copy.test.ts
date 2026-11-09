import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Storage} from '../../zql/src/ivm/operator.ts';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.ts';
import {Database} from './db.ts';

const lc = createSilentLogContext();

function newStorage(maxMemoryBytes: number) {
  const db = new Database(lc, ':memory:');
  db.prepare(CREATE_STORAGE_TABLE).run();
  return new DatabaseStorage(db, {maxMemoryBytes});
}

const entries = (s: Storage) => [...s.scan()];

describe.each([
  ['in memory', 1024 * 1024],
  ['in SQLite', 0],
])('copyStorage %s', (_name, maxMemoryBytes) => {
  test('copies the values of another group', () => {
    const storage = newStorage(maxMemoryBytes);
    const source = storage.createClientGroupStorage('a').createStorage();
    source.set('k1', {size: 1});
    source.set('k2', [1, 2]);
    const group = storage.createClientGroupStorage('b');
    const target = group.createStorage();
    expect(group.copyStorage(source, target)).toBe(2);
    expect(entries(target)).toEqual(entries(source));
  });

  test('replaces what the target held', () => {
    const storage = newStorage(maxMemoryBytes);
    const source = storage.createClientGroupStorage('a').createStorage();
    source.set('k1', 1);
    const group = storage.createClientGroupStorage('b');
    const target = group.createStorage();
    target.set('k0', 0);
    target.set('k1', 0);
    group.copyStorage(source, target);
    expect(entries(target)).toEqual([['k1', 1]]);
  });

  test('leaves the two independent', () => {
    const storage = newStorage(maxMemoryBytes);
    const source = storage.createClientGroupStorage('a').createStorage();
    source.set('k1', 1);
    const group = storage.createClientGroupStorage('b');
    const target = group.createStorage();
    group.copyStorage(source, target);
    source.set('k2', 2);
    target.del('k1');
    target.set('k3', 3);
    expect(entries(source)).toEqual([
      ['k1', 1],
      ['k2', 2],
    ]);
    expect(entries(target)).toEqual([['k3', 3]]);
  });

  test('a copy survives its source', () => {
    const storage = newStorage(maxMemoryBytes);
    const sourceGroup = storage.createClientGroupStorage('a');
    const source = sourceGroup.createStorage();
    source.set('k1', 1);
    const group = storage.createClientGroupStorage('b');
    const target = group.createStorage();
    group.copyStorage(source, target);
    sourceGroup.destroy();
    expect(entries(target)).toEqual([['k1', 1]]);
  });

  test('does not copy from another storage', () => {
    const source = newStorage(maxMemoryBytes)
      .createClientGroupStorage('a')
      .createStorage();
    const group = newStorage(maxMemoryBytes).createClientGroupStorage('b');
    expect(group.copyStorage(source, group.createStorage())).toBeUndefined();
  });

  test('only copies into its own group', () => {
    const storage = newStorage(maxMemoryBytes);
    const source = storage.createClientGroupStorage('a').createStorage();
    const other = storage.createClientGroupStorage('c').createStorage();
    const group = storage.createClientGroupStorage('b');
    expect(() => group.copyStorage(source, other)).toThrow();
  });
});

test('copyStorage counts a copy in memory against the budget', () => {
  const storage = newStorage(1024 * 1024);
  const source = storage.createClientGroupStorage('a').createStorage();
  for (let i = 0; i < 100; i++) {
    source.set(`k${i}`, {size: i});
  }
  const before = storage.memoryBytes;
  const group = storage.createClientGroupStorage('b');
  group.copyStorage(source, group.createStorage());
  expect(storage.memoryBytes).toBe(before * 2);
});

test('copyStorage spills when a copy goes over the budget', () => {
  const storage = newStorage(1024);
  const source = storage.createClientGroupStorage('a').createStorage();
  for (let i = 0; i < 10; i++) {
    source.set(`k${i}`, i);
  }
  expect(storage.memoryBytes).toBeLessThan(1024);
  const group = storage.createClientGroupStorage('b');
  const targets = Array.from({length: 20}, () => group.createStorage());
  for (const target of targets) {
    group.copyStorage(source, target);
  }
  expect(storage.memoryBytes).toBeLessThanOrEqual(1024);
  for (const target of targets) {
    expect(entries(target)).toEqual(entries(source));
  }
});

test('copyStorage copies from a spilled source', () => {
  const storage = newStorage(64);
  const sourceGroup = storage.createClientGroupStorage('a');
  const source = sourceGroup.createStorage();
  for (let i = 0; i < 10; i++) {
    source.set(`k${i}`, {value: 'x'.repeat(10)});
  }
  // Over the budget on its own, so it has spilled.
  expect(storage.memoryBytes).toBe(0);
  const group = storage.createClientGroupStorage('b');
  const target = group.createStorage();
  expect(group.copyStorage(source, target)).toBe(10);
  expect(entries(target)).toEqual(entries(source));
});
