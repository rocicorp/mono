import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import type {InternalDiff} from '../btree/node.ts';
import {TestStore} from '../dag/test-store.ts';
import {ChainBuilder} from '../db/test-helpers.ts';
import {newWriteSnapshotDD31} from '../db/write.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {PatchOperationInternal} from '../patch-operation.ts';
import {withWriteNoImplicitCommit} from '../with-transactions.ts';
import {apply} from './patch.ts';

const formatVersion = FormatVersion.Latest;
const clientID = 'client-id';
const lc = new LogContext();

function makeEntityValue(i: number, version: number) {
  return {
    id: `ent_${String(i).padStart(6, '0')}`,
    shortID: i + 1000000,
    title: `Entity title number ${i} with some extra padding text to make it realistic`,
    open: i % 3 !== 0,
    modified: 1769711510846 + i,
    created: 1767179995185 + i,
    creatorID: `usr_${String(i % 100).padStart(4, '0')}`,
    assigneeID: `usr_${String((i + 37) % 100).padStart(4, '0')}`,
    description: `Description for entity ${i}. This is some filler text to make the value realistically sized.`,
    visibility: 'public',
    projectID: `proj_${String(i % 50).padStart(3, '0')}`,
    version,
  };
}

describe('putMany diff correctness', () => {
  const diffConfig = {
    shouldComputeDiffs: () => true,
    shouldComputeDiffsForIndex: () => false,
  };

  // This reproduces the production scenario:
  // 1. Initial sync: small poke applies via putMany → becomes main head
  // 2. Later poke: big patch with overlapping keys applied via putMany
  // 3. Diff between main head and sync head should be correct
  //
  // The key difference from the previous test: BOTH trees are built via
  // putMany (simulating multiple poke rounds through the same code path).
  test('putMany on both base and new tree', async () => {
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    // Step 1: Simulate poke 1-3 building base via apply (putMany)
    const basePatch: PatchOperationInternal[] = [];
    for (let i = 0; i < 103; i++) {
      const key = `e/entity/ent_${String(i).padStart(6, '0')}`;
      basePatch.push({op: 'put', key, value: makeEntityValue(i, 1)});
    }
    // Also add some non-entity keys (like desired queries, got queries)
    for (let i = 0; i < 3; i++) {
      basePatch.push({
        op: 'put',
        key: `d/${clientID}/hash${i}`,
        value: null,
      });
    }

    const baseHash = await withWriteNoImplicitCommit(store, async dagWrite => {
      const dbWrite = await newWriteSnapshotDD31(
        b.chain[0].chunk.hash,
        {[clientID]: 1},
        'cookie1',
        dagWrite,
        clientID,
        formatVersion,
      );
      await apply(lc, dbWrite, basePatch);
      return dbWrite.commit('main');
    });

    // Step 2: Simulate poke 4 with 4913 puts, overlapping base
    const bigPatch: PatchOperationInternal[] = [];
    // All entity keys from 0-4912, overlapping 0-102 with base
    for (let i = 0; i < 4913; i++) {
      const key = `e/entity/ent_${String(i).padStart(6, '0')}`;
      bigPatch.push({
        op: 'put',
        key,
        value: makeEntityValue(i, i < 103 ? 2 : 1),
      });
    }

    // Apply via putMany (the optimized path)
    const [, optimizedDiffsMap] = await withWriteNoImplicitCommit(
      store,
      async dagWrite => {
        const dbWrite = await newWriteSnapshotDD31(
          baseHash,
          {[clientID]: 1},
          'cookie2',
          dagWrite,
          clientID,
          formatVersion,
        );
        await apply(lc, dbWrite, bigPatch);
        return dbWrite.commitWithDiffs('sync', diffConfig);
      },
    );
    const optimizedDiffs: InternalDiff = optimizedDiffsMap.get('') ?? [];

    // Apply via sequential put
    const [, sequentialDiffsMap] = await withWriteNoImplicitCommit(
      store,
      async dagWrite => {
        const dbWrite = await newWriteSnapshotDD31(
          baseHash,
          {[clientID]: 1},
          'cookie3',
          dagWrite,
          clientID,
          formatVersion,
        );
        for (const op of bigPatch) {
          if (op.op === 'put') {
            await dbWrite.put(lc, op.key, deepFreeze(op.value));
          }
        }
        return dbWrite.commitWithDiffs('sync2', diffConfig);
      },
    );
    const sequentialDiffs: InternalDiff = sequentialDiffsMap.get('') ?? [];

    // Validate
    const baseKeys = new Set<string>();
    for (let i = 0; i < 103; i++) {
      baseKeys.add(`e/entity/ent_${String(i).padStart(6, '0')}`);
    }

    const wrongAdds = optimizedDiffs.filter(
      d => d.op === 'add' && baseKeys.has(d.key),
    );
    expect(
      wrongAdds.map(d => d.key),
      `putMany produced ${wrongAdds.length} wrong add diffs for existing keys`,
    ).toEqual([]);

    expect(optimizedDiffs.length).toBe(sequentialDiffs.length);
  });
});
