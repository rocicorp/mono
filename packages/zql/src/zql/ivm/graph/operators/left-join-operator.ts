import {genCached, genConcat, genFlatMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import {
  getPrimaryKeyValuesAsStringUnqualified,
  getValueFromEntity,
} from '../../source/util.js';
import {
  isJoinResult,
  JoinResult,
  PipelineEntity,
  StringOrNumber,
  Version,
} from '../../types.js';
import {combineRows, DifferenceIndex} from './difference-index.js';
import {JoinOperatorBase} from './join-operator-base.js';
import type {JoinArgs} from './join-operator.js';

export class LeftJoinOperator<
  AValue extends PipelineEntity,
  BValue extends PipelineEntity,
  ATable extends string | undefined,
  BAlias extends string | undefined,
> extends JoinOperatorBase<
  AValue,
  BValue,
  // If AValue or BValue are join results
  // then they should be lifted and need no aliasing
  // since they're already aliased
  JoinResult<AValue, BValue, ATable, BAlias>
> {
  readonly #indexA: DifferenceIndex<StringOrNumber | undefined, AValue>;
  readonly #indexB: DifferenceIndex<StringOrNumber, BValue>;
  readonly #aMatches: Map<
    StringOrNumber,
    [JoinResult<AValue, BValue, ATable, BAlias>, number]
  > = new Map();
  readonly #getAPrimaryKey;
  readonly #getBPrimaryKey;
  readonly #getAJoinKey;
  readonly #getBJoinKey;
  readonly #joinArgs: JoinArgs<AValue, BValue, ATable, BAlias>;

  constructor(joinArgs: JoinArgs<AValue, BValue, ATable, BAlias>) {
    super(joinArgs.a, joinArgs.b, joinArgs.output, (version, inputA, inputB) =>
      this.#join(version, inputA, inputB),
    );

    this.#getAPrimaryKey = (value: AValue) =>
      getPrimaryKeyValuesAsStringUnqualified(
        value,
        joinArgs.aPrimaryKeyColumns,
      );
    this.#getBPrimaryKey = (value: BValue) =>
      getPrimaryKeyValuesAsStringUnqualified(
        value,
        joinArgs.bPrimaryKeyColumns,
      );

    this.#getAJoinKey = (value: AValue) =>
      getValueFromEntity(value, joinArgs.aJoinColumn) as StringOrNumber;
    this.#getBJoinKey = (value: BValue) =>
      getValueFromEntity(value, joinArgs.bJoinColumn) as StringOrNumber;

    this.#indexA = new DifferenceIndex<StringOrNumber, AValue>(
      this.#getAPrimaryKey,
    );
    this.#indexB = new DifferenceIndex<StringOrNumber, BValue>(
      this.#getBPrimaryKey,
    );

    this.#joinArgs = joinArgs;
  }

  #aKeysForCompaction = new Set<StringOrNumber | undefined>();
  #bKeysForCompaction = new Set<StringOrNumber>();
  #lastVersion = -1;
  #join(
    version: Version,
    inputA: Multiset<AValue> | undefined,
    inputB: Multiset<BValue> | undefined,
  ) {
    if (this.#lastVersion !== version) {
      // TODO: all outstanding iterables _must_ be made invalid before processing a new version.
      // We should add some invariant in `joinOne` that checks if the version is still valid
      // and throws if not.
      this.#indexA.compact(this.#aKeysForCompaction);
      this.#indexB.compact(this.#bKeysForCompaction);
      this.#aKeysForCompaction.clear();
      this.#bKeysForCompaction.clear();
      this.#lastVersion = version;
    }

    const iterablesToReturn: Multiset<
      JoinResult<AValue, BValue, ATable, BAlias>
    >[] = [];

    // fill the inner set first so we don't emit 2x the amount of data
    // I.e., so we don't omit `null` values for each `a` value followed by
    // the actual join results.
    if (inputB !== undefined) {
      iterablesToReturn.push(
        genFlatMap(inputB, entry => {
          const key = this.#getBJoinKey(entry[0]);
          const ret = this.#joinOneInner(entry, key);
          if (key !== undefined) {
            this.#indexB.add(key, entry);
            this.#bKeysForCompaction.add(key);
          }
          return ret;
        }),
      );
    }

    if (inputA !== undefined) {
      iterablesToReturn.push(
        genFlatMap(inputA, entry => {
          const key = this.#getAJoinKey(entry[0]);
          const ret = this.#joinOneLeft(entry, key);
          this.#indexA.add(key, entry);
          this.#aKeysForCompaction.add(key);
          return ret;
        }),
      );
    }

    return genCached(genConcat(iterablesToReturn));
  }

  #joinOneLeft(
    aEntry: Entry<AValue>,
    aKey: StringOrNumber | undefined,
  ): Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] {
    const aValue = aEntry[0];
    const aMult = aEntry[1];

    const ret: Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] = [];
    const aPrimaryKey = isJoinResult(aValue)
      ? aValue.id
      : this.#getAPrimaryKey(aValue as AValue);

    const bEntries = aKey !== undefined ? this.#indexB.get(aKey) : undefined;
    // `undefined` cannot join with anything
    if (bEntries === undefined || bEntries.length === 0) {
      const joinEntry = [
        combineRows(
          aValue,
          undefined,
          this.#joinArgs.aTable,
          this.#joinArgs.bAs,
          this.#getAPrimaryKey,
          this.#getBPrimaryKey,
        ) as JoinResult<AValue, BValue, ATable, BAlias>,
        aMult,
      ] as const;
      ret.push(joinEntry);
      this.#aMatches.set(aPrimaryKey, [joinEntry[0], 0]);
      return ret;
    }

    for (const [bValue, bMult] of bEntries) {
      const joinEntry = [
        combineRows(
          aValue,
          bValue,
          this.#joinArgs.aTable,
          this.#joinArgs.bAs,
          this.#getAPrimaryKey,
          this.#getBPrimaryKey,
        ) as JoinResult<AValue, BValue, ATable, BAlias>,
        aMult * bMult,
      ] as const;

      ret.push(joinEntry);

      const existing = this.#aMatches.get(aPrimaryKey);
      if (existing) {
        existing[1] += joinEntry[1];
      } else {
        this.#aMatches.set(aPrimaryKey, [joinEntry[0], joinEntry[1]]);
      }
    }

    return ret;
  }

  #joinOneInner(
    bEntry: Entry<BValue>,
    bKey: StringOrNumber | undefined,
  ): Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] {
    const bValue = bEntry[0];
    const bMult = bEntry[1];
    if (bKey === undefined) {
      return [];
    }

    const aEntries = this.#indexA.get(bKey);
    if (aEntries === undefined) {
      return [];
    }

    const ret: Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] = [];
    for (const [aRow, aMult] of aEntries) {
      const joinEntry = [
        combineRows(
          aRow,
          bValue,
          this.#joinArgs.aTable,
          this.#joinArgs.bAs,
          this.#getAPrimaryKey,
          this.#getBPrimaryKey,
        ) as JoinResult<AValue, BValue, ATable, BAlias>,
        aMult * bMult,
      ] as const;
      ret.push(joinEntry);

      const aPrimaryKey = isJoinResult(aRow)
        ? aRow.id
        : this.#getAPrimaryKey(aRow as AValue);

      const existing = this.#aMatches.get(aPrimaryKey);
      if (joinEntry[1] > 0 && existing && existing[1] === 0) {
        // row `a` now has matches. Remove the un-match.
        ret.push([existing[0], -1]);
      } else if (
        joinEntry[1] < 0 &&
        existing &&
        existing[1] + joinEntry[1] === 0
      ) {
        // We went back to row `a` being an unmatch. Send the un-match
        ret.push([existing[0], 1]);
      }

      if (existing) {
        existing[1] += joinEntry[1];
      }
    }

    return ret;
  }

  toString() {
    return `indexa: ${this.#indexA.toString()}\n\n\nindexb: ${this.#indexB.toString()}`;
  }
}
