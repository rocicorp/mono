import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import type * as dag from '../dag/mod';
import {Commit, DEFAULT_HEAD_NAME, Meta} from './commit';
import {readCommit, whenceHead} from './read';
import {initDB, Write, readIndexesForWrite} from './write';
import type {JSONValue} from '../json';

export type Chain = Commit<Meta>[];

export async function addGenesis(
  chain: Chain,
  store: dag.Store,
): Promise<Chain> {
  expect(chain).to.have.length(0);
  const commit = await createGenesis(store);
  chain.push(commit);
  return chain;
}

export async function createGenesis(store: dag.Store): Promise<Commit<Meta>> {
  await store.withWrite(async w => {
    await initDB(w, DEFAULT_HEAD_NAME);
  });
  return await store.withRead(async read => {
    const [, commit] = await readCommit(whenceHead(DEFAULT_HEAD_NAME), read);
    return commit;
  });
}

// Local commit has mutator name and args according to its index in the
// chain.
export async function addLocal(
  chain: Chain,
  store: dag.Store,
  entries?: [string, JSONValue][],
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createLocal(entries ?? [[`local`, `${i}`]], store, i);

  chain.push(commit);
  return chain;
}

export async function createLocal(
  entries: [string, JSONValue][],
  store: dag.Store,
  i: number,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    const w = await Write.newLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      `mutator_name_${i}`,
      [i],
      null,
      dagWrite,
      42,
    );
    for (const [key, val] of entries) {
      await w.put(lc, key, val);
    }
    await w.commit(DEFAULT_HEAD_NAME);
  });
  return await store.withRead(async dagRead => {
    const [, commit] = await readCommit(whenceHead(DEFAULT_HEAD_NAME), dagRead);
    return commit;
  });
}

export async function addIndexChange(
  chain: Chain,
  store: dag.Store,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createIndex(i + '', 'local', '', store, false);
  chain.push(commit);
  return chain;
}

export async function createIndex(
  name: string,
  prefix: string,
  jsonPointer: string,
  store: dag.Store,
  allowEmpty: boolean,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    const w = await Write.newIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
    );
    await w.createIndex(lc, name, prefix, jsonPointer, allowEmpty);
    await w.commit(DEFAULT_HEAD_NAME);
  });
  return store.withRead(async dagRead => {
    const [, commit] = await readCommit(whenceHead(DEFAULT_HEAD_NAME), dagRead);
    return commit;
  });
}

// See also sync.test_helpers for add_sync_snapshot, which can't go here because
// it depends on details of sync and sync depends on db.

// The optional map for the commit is treated as key, value pairs.
export async function addSnapshot(
  chain: Chain,
  store: dag.Store,
  map: [string, JSONValue][] | undefined,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  const cookie = `cookie_${chain.length}`;
  await store.withWrite(async dagWrite => {
    const w = await Write.newSnapshot(
      whenceHead(DEFAULT_HEAD_NAME),
      chain[chain.length - 1].nextMutationID,
      cookie,
      dagWrite,
      readIndexesForWrite(chain[chain.length - 1]),
    );

    if (map) {
      for (const [k, v] of map) {
        await w.put(lc, k, v);
      }
    }
    await w.commit(DEFAULT_HEAD_NAME);
  });
  return store.withRead(async dagRead => {
    const [, commit] = await readCommit(whenceHead(DEFAULT_HEAD_NAME), dagRead);
    chain.push(commit);
    return chain;
  });
}
