import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import type * as dag from '../dag/mod';
import {Commit, DEFAULT_HEAD_NAME, Meta, MetaType} from './commit';
import {readCommit, whenceHead} from './read';
import {
  Write,
  readIndexesForWrite,
  newWriteSnapshot,
  newWriteSnapshotDD31,
  newWriteLocal,
  newWriteIndexChange,
} from './write';
import type {JSONValue} from '../json';
import {toInternalValue, ToInternalValueReason} from '../internal-value.js';
import type {ClientID} from '../sync/client-id.js';
import {emptyHash, Hash} from '../hash.js';
import {BTreeWrite} from '../btree/mod.js';

export type Chain = Commit<Meta>[];

export async function addGenesis(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
): Promise<Chain> {
  expect(chain).to.have.length(0);
  const commit = await createGenesis(store, clientID);
  chain.push(commit);
  return chain;
}

export async function createGenesis(
  store: dag.Store,
  clientID: ClientID,
): Promise<Commit<Meta>> {
  await store.withWrite(async w => {
    await initDB(w, DEFAULT_HEAD_NAME, clientID);
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
  clientID: ClientID,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createLocal([[`local`, `${i}`]], store, i, clientID);

  chain.push(commit);
  return chain;
}

export async function createLocal(
  entries: [string, JSONValue][],
  store: dag.Store,
  i: number,
  clientID: ClientID,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      `mutator_name_${i}`,
      toInternalValue([i], ToInternalValueReason.Test),
      null,
      dagWrite,
      42,
      clientID,
    );
    for (const [key, val] of entries) {
      await w.put(lc, key, toInternalValue(val, ToInternalValueReason.Test));
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
  clientID: ClientID,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createIndex(i + '', 'local', '', store, false, clientID);
  chain.push(commit);
  return chain;
}

export async function createIndex(
  name: string,
  prefix: string,
  jsonPointer: string,
  store: dag.Store,
  allowEmpty: boolean,
  clientID: ClientID,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    const w = await newWriteIndexChange(
      whenceHead(DEFAULT_HEAD_NAME),
      dagWrite,
      clientID,
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
  clientID: ClientID,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  const cookie = `cookie_${chain.length}`;
  await store.withWrite(async dagWrite => {
    let w;
    if (DD31) {
      w = await newWriteSnapshotDD31(
        whenceHead(DEFAULT_HEAD_NAME),
        {
          [clientID]: await chain[chain.length - 1].getNextMutationID(
            clientID,
            dagWrite,
          ),
        },
        cookie,
        dagWrite,
        readIndexesForWrite(chain[chain.length - 1], dagWrite),
        clientID,
      );
    } else {
      w = await newWriteSnapshot(
        whenceHead(DEFAULT_HEAD_NAME),
        await chain[chain.length - 1].getNextMutationID(clientID, dagWrite),
        cookie,
        dagWrite,
        readIndexesForWrite(chain[chain.length - 1], dagWrite),
        clientID,
      );
    }

    if (map) {
      for (const [k, v] of map) {
        await w.put(lc, k, toInternalValue(v, ToInternalValueReason.Test));
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

export async function initDB(
  dagWrite: dag.Write,
  headName: string,
  clientID: ClientID,
): Promise<Hash> {
  const basisHash = emptyHash;
  if (DD31) {
    const w = new Write(
      dagWrite,
      new BTreeWrite(dagWrite),
      undefined,
      {
        basisHash,
        type: MetaType.Snapshot,
        lastMutationIDs: {[clientID]: 0},
        cookieJSON: null,
      },
      new Map(),
      clientID,
    );
    return await w.commit(headName);
  }
  const w = new Write(
    dagWrite,
    new BTreeWrite(dagWrite),
    undefined,
    {basisHash, type: MetaType.Snapshot, lastMutationID: 0, cookieJSON: null},
    new Map(),
    clientID,
  );
  return await w.commit(headName);
}
