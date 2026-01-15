import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {diff as btreeDiff} from '../btree/diff.ts';
import type {InternalDiff} from '../btree/node.ts';
import {allEntriesAsDiff, BTreeRead} from '../btree/read.ts';
import type {Read} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {commitFromHash, type Meta} from '../db/commit.ts';
import {readIndexesForRead} from '../db/read.ts';
import type * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';

type FormatVersion = Enum<typeof FormatVersion>;

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
  read: Read,
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
  read: Read,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
): Promise<DiffsMap> {
  const diffsMap = new DiffsMap();
  if (!diffConfig.shouldComputeDiffs()) {
    return diffsMap;
  }

  const oldMap = new BTreeRead(read, formatVersion, oldCommit.valueHash);
  const newMap = new BTreeRead(read, formatVersion, newCommit.valueHash);
  const valueDiff = await btreeDiff(oldMap, newMap);
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
  read: Read,
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
      const diffs = await btreeDiff(oldIndex.map, newIndex.map);
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
