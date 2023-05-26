import {assert} from 'shared/asserts.js';
import * as btree from '../btree/mod.js';
import type {InternalDiff} from '../btree/node.js';
import {allEntriesAsDiff, BTreeRead} from '../btree/read.js';
import type * as dag from '../dag/mod.js';
import {Commit, commitFromHash, Meta} from '../db/commit.js';
import {readIndexesForRead} from '../db/read.js';
import type {FormatVersion} from '../format-version.js';
import type {Hash} from '../hash.js';

/**
 * Interface allowing different diff functions to skip costly diff computations.
 */
export interface DiffComputationConfig {
  shouldComputeDiffs(): boolean;
  shouldComputeDiffsForIndex(name: string): boolean;
}

/**
 * The diffs in different indexes. The key of the map is the index name.
 * "" is used for the primary index.
 */
export class DiffsMap extends Map<string, InternalDiff> {
  override set(key: string, value: InternalDiff): this {
    if (value.length === 0) {
      return this;
    }
    return super.set(key, value);
  }
}

/**
 * Diffs the state of the db at two different hashes.
 * It will include the primary indexes as well as all the secondary indexes.
 */
export async function diff(
  oldHash: Hash,
  newHash: Hash,
  read: dag.Read,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
): Promise<DiffsMap> {
  const [oldCommit, newCommit] = await Promise.all([
    commitFromHash(oldHash, read),
    commitFromHash(newHash, read),
  ]);

  return diffCommits(oldCommit, newCommit, read, diffConfig, formatVersion);
}

/**
 * Diffs the state of the db at two different commits.
 * It will include the primary indexes as well as all the secondary indexes.
 */
// TODO: this should probably move to db/
export async function diffCommits(
  oldCommit: Commit<Meta>,
  newCommit: Commit<Meta>,
  read: dag.Read,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
): Promise<DiffsMap> {
  const diffsMap = new DiffsMap();
  if (!diffConfig.shouldComputeDiffs()) {
    return diffsMap;
  }

  const oldMap = new BTreeRead(read, formatVersion, oldCommit.valueHash);
  const newMap = new BTreeRead(read, formatVersion, newCommit.valueHash);
  const valueDiff = await btree.diff(oldMap, newMap);
  diffsMap.set('', valueDiff);

  await addDiffsForIndexes(
    oldCommit,
    newCommit,
    read,
    diffsMap,
    diffConfig,
    formatVersion,
  );

  return diffsMap;
}

export async function addDiffsForIndexes(
  mainCommit: Commit<Meta>,
  syncCommit: Commit<Meta>,
  read: dag.Read,
  diffsMap: DiffsMap,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
) {
  const oldIndexes = readIndexesForRead(mainCommit, read, formatVersion);
  const newIndexes = readIndexesForRead(syncCommit, read, formatVersion);

  for (const [oldIndexName, oldIndex] of oldIndexes) {
    if (!diffConfig.shouldComputeDiffsForIndex(oldIndexName)) {
      continue;
    }

    const newIndex = newIndexes.get(oldIndexName);
    if (newIndex !== undefined) {
      assert(newIndex !== oldIndex);
      const diffs = await btree.diff(oldIndex.map, newIndex.map);
      newIndexes.delete(oldIndexName);
      diffsMap.set(oldIndexName, diffs);
    } else {
      // old index name is not in the new indexes. All entries removed!
      const diffs = await allEntriesAsDiff(oldIndex.map, 'del');
      diffsMap.set(oldIndexName, diffs);
    }
  }

  for (const [newIndexName, newIndex] of newIndexes) {
    if (!diffConfig.shouldComputeDiffsForIndex(newIndexName)) {
      continue;
    }
    // new index name is not in the old indexes. All keys added!
    const diffs = await allEntriesAsDiff(newIndex.map, 'add');
    diffsMap.set(newIndexName, diffs);
  }
}
