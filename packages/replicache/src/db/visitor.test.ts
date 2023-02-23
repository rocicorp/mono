import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod.js';
import type * as sync from '../sync/mod.js';
import {ChainBuilder} from './test-helpers.js';
import {fakeHash, Hash} from '../hash.js';
import type {Entry, Node} from '../btree/node.js';
import {FrozenJSONValue, ReadonlyJSONValue, deepFreeze} from '../json.js';
import {Visitor} from './visitor.js';
import {
  baseSnapshotHashFromHash,
  Commit,
  IndexRecord,
  Meta,
  newLocalDD31,
  newLocalSDD,
} from './commit.js';
import {promiseVoid} from '../resolved-promises.js';
import {withRead, withWrite} from '../with-transactions.js';

function newLocal(
  createChunk: dag.CreateChunk,
  basisHash: Hash,
  baseSnapshotHash: Hash,
  mutationID: number,
  mutatorName: string,
  mutatorArgsJSON: FrozenJSONValue,
  originalHash: Hash | null,
  valueHash: Hash,
  indexes: readonly IndexRecord[],
  timestamp: number,
  clientID: sync.ClientID,
  dd31: boolean,
) {
  if (dd31) {
    return newLocalDD31(
      createChunk,
      basisHash,
      baseSnapshotHash,
      mutationID,
      mutatorName,
      mutatorArgsJSON,
      originalHash,
      valueHash,
      indexes,
      timestamp,
      clientID,
    );
  }
  return newLocalSDD(
    createChunk,
    basisHash,
    mutationID,
    mutatorName,
    mutatorArgsJSON,
    originalHash,
    valueHash,
    indexes,
    timestamp,
  );
}

suite('test that we get to the data nodes', () => {
  const t = async (dd31: boolean) => {
    const clientID = 'client-id';
    const dagStore = new dag.TestStore();

    const log: (readonly Entry<Hash>[] | readonly Entry<FrozenJSONValue>[])[] =
      [];
    const b = new ChainBuilder(dagStore, undefined, dd31);

    class TestVisitor extends Visitor {
      override visitBTreeNodeChunk(chunk: dag.Chunk<Node>) {
        log.push(chunk.data[1]);
        return promiseVoid;
      }
    }

    const t = async (commit: Commit<Meta>, expected: ReadonlyJSONValue[]) => {
      log.length = 0;
      await withRead(dagStore, async dagRead => {
        const visitor = new TestVisitor(dagRead);
        await visitor.visitCommit(commit.chunk.hash);
        expect(log).to.deep.equal(expected);
      });
    };

    if (dd31) {
      await b.addGenesis(clientID, {
        1: {prefix: 'local', jsonPointer: '', allowEmpty: false},
      });
      await t(b.chain[0], [[], []]);
    } else {
      await b.addGenesis(clientID);
      await t(b.chain[0], [[]]);
    }

    await b.addLocal(clientID);
    if (dd31) {
      await t(b.chain[1], [
        [['local', '1']],
        [['\u00001\u0000local', '1']],
        [],
        [],
      ]);
    } else {
      await t(b.chain[1], [[['local', '1']], []]);
    }

    if (dd31) {
      await b.addSnapshot(undefined, clientID, undefined, undefined);
      await t(b.chain[2], [[['local', '1']], [['\u00001\u0000local', '1']]]);
    } else {
      await b.addIndexChange(clientID);
      await t(b.chain[2], [
        [['local', '1']],
        [['\u00001\u0000local', '1']],
        [],
      ]);
    }

    await b.addLocal(clientID);
    if (dd31) {
      await t(b.chain[3], [
        [['local', '3']],
        [['\u00003\u0000local', '3']],
        [['local', '1']],
        [['\u00001\u0000local', '1']],
      ]);
    } else {
      await t(b.chain[3], [
        [['local', '3']],
        [['\u00003\u0000local', '3']],
        [['local', '1']],
        [['\u00001\u0000local', '1']],
        [],
      ]);
    }

    await b.addSnapshot([['k', 42]], clientID);
    await t(b.chain[4], [
      [
        ['k', 42],
        ['local', '3'],
      ],
      [['\u00003\u0000local', '3']],
    ]);

    await b.addLocal(clientID);
    const syncChain = await b.addSyncSnapshot(b.chain.length - 1, clientID);
    if (dd31) {
      await t(syncChain[0], [
        [
          ['k', 42],
          ['local', '3'],
        ],
        [['\u00003\u0000local', '3']],
      ]);
    } else {
      await t(syncChain[0], [
        [
          ['k', 42],
          ['local', '3'],
        ],
        [['\u00005\u0000local', '5']],
        [['\u00003\u0000local', '3']],
      ]);
    }

    const localCommit = await withWrite(dagStore, async dagWrite => {
      const prevCommit = b.chain[b.chain.length - 1];
      const baseSnapshotHash = await baseSnapshotHashFromHash(
        prevCommit.chunk.hash,
        dagWrite,
      );
      const localCommit = newLocal(
        dagWrite.createChunk,
        prevCommit.chunk.hash,
        baseSnapshotHash,
        42,
        'mutator-name',
        deepFreeze([]),
        fakeHash('0e'),
        prevCommit.valueHash,
        prevCommit.indexes,
        88,
        clientID,
        dd31,
      );
      await dagWrite.putChunk(localCommit.chunk);
      await dagWrite.setHead('test', localCommit.chunk.hash);
      await dagWrite.commit();
      return localCommit;
    });
    await t(localCommit, [
      [
        ['k', 42],
        ['local', '5'],
      ],
      [['\u00005\u0000local', '5']],
      [
        ['k', 42],
        ['local', '3'],
      ],
      [['\u00003\u0000local', '3']],
    ]);

    const localCommit2 = await withWrite(dagStore, async dagWrite => {
      const prevCommit = b.chain[b.chain.length - 1];
      const baseSnapshotHash = await baseSnapshotHashFromHash(
        prevCommit.chunk.hash,
        dagWrite,
      );
      const localCommit2 = newLocal(
        dagWrite.createChunk,
        prevCommit.chunk.hash,
        baseSnapshotHash,
        42,
        'mutator-name',
        deepFreeze([]),
        localCommit.chunk.hash,
        prevCommit.valueHash,
        prevCommit.indexes,
        88,
        clientID,
        dd31,
      );
      await dagWrite.putChunk(localCommit2.chunk);
      await dagWrite.setHead('test2', localCommit2.chunk.hash);
      await dagWrite.commit();
      return localCommit2;
    });
    await t(localCommit2, [
      [
        ['k', 42],
        ['local', '5'],
      ],
      [['\u00005\u0000local', '5']],
      [
        ['k', 42],
        ['local', '3'],
      ],
      [['\u00003\u0000local', '3']],
    ]);
  };
  test('dd31', () => t(true));
  test('sdd', () => t(false));
});
