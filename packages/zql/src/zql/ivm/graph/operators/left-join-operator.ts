import type {Ordering} from '../../../ast/ast.js';
import {genCached, genConcat, genFlatMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import type {Source} from '../../source/source.js';
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
import {
  DifferenceIndex,
  MemoryBackedDifferenceIndex,
} from './difference-index.js';
import {JoinOperatorBase} from './join-operator-base.js';
import {makeJoinResult, JoinArgs} from './join-operator.js';
import {SourceBackedDifferenceIndex} from './source-backed-difference-index.js';

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
  readonly #indexA: MemoryBackedDifferenceIndex<
    StringOrNumber | undefined,
    AValue
  >;
  readonly #indexB: DifferenceIndex<StringOrNumber, BValue>;

  // Tracks the cumulative multiplicity of each row in A that we have seen.
  // Example with issues and comments: the issue will be side A and the child
  // comments will be side B. As issue rows flow through join we accumulate
  // their multiplicity here, keyed by A's PK.
  //
  // The reason to do this is so that we know when to retract a join result
  // where A had no child B rows. See comment in #joinOneInner.
  readonly #aMatches: Map<
    StringOrNumber,
    [JoinResult<AValue, BValue, ATable, BAlias>, number]
  > = new Map();

  readonly #getAPrimaryKey;
  readonly #getBPrimaryKey;
  readonly #getAJoinKey;
  readonly #getBJoinKey;
  readonly #joinArgs: JoinArgs<AValue, BValue, ATable, BAlias>;

  constructor(
    joinArgs: JoinArgs<AValue, BValue, ATable, BAlias>,
    sourceProvider:
      | ((
          sourceName: string,
          order: Ordering | undefined,
        ) => Source<PipelineEntity>)
      | undefined,
  ) {
    super(
      joinArgs.a,
      joinArgs.b,
      joinArgs.output,
      (version, inputA, inputB, isHistory) =>
        this.#join(version, inputA, inputB, isHistory),
      joinArgs.aJoinColumn,
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
      // TODO(aa): bad cast, this can be undefined
      // Actually maybe it could be any value? does something earlier in
      // pipeline enforce that it's a string or number?
      getValueFromEntity(value, joinArgs.aJoinColumn) as StringOrNumber;
    this.#getBJoinKey = (value: BValue) =>
      getValueFromEntity(value, joinArgs.bJoinColumn) as StringOrNumber;

    this.#indexA = new MemoryBackedDifferenceIndex<StringOrNumber, AValue>(
      this.#getAPrimaryKey,
    );

    // load indexB from the source...
    if (sourceProvider === undefined) {
      this.#indexB = new MemoryBackedDifferenceIndex<StringOrNumber, BValue>(
        this.#getBPrimaryKey,
      );
    } else {
      const sourceB = sourceProvider(joinArgs.bTable, undefined);
      this.#indexB = new SourceBackedDifferenceIndex(
        sourceB.getOrCreateAndMaintainNewHashIndex(joinArgs.bJoinColumn),
      ) as SourceBackedDifferenceIndex<StringOrNumber, BValue>;
    }

    this.#joinArgs = joinArgs;
  }

  #lastVersion = -1;
  #join(
    version: Version,
    inputA: Multiset<AValue> | undefined,
    inputB: Multiset<BValue> | undefined,
    isHistory: boolean,
  ) {
    if (this.#lastVersion !== version) {
      // TODO: all outstanding iterables _must_ be made invalid before processing a new version.
      // We should add some invariant in `joinOne` that checks if the version is still valid
      // and throws if not.
      this.#indexA.compact();
      this.#indexB.compact();
      this.#lastVersion = version;
    }

    const iterablesToReturn: Multiset<
      JoinResult<AValue, BValue, ATable, BAlias>
    >[] = [];

    // fill the inner set first so we don't emit 2x the amount of data
    // I.e., so we don't omit `null` values for each `a` value followed by
    // the actual join results.
    //
    // Don't iterate over `inputB` in history mode.
    // It is already filled in that case and the join from `a` will get everything.
    if (inputB !== undefined && !isHistory) {
      iterablesToReturn.push(
        genFlatMap(inputB, entry => {
          const key = this.#getBJoinKey(entry[0]);
          const ret = this.#joinOneInner(entry, key);
          if (key !== undefined) {
            this.#indexB.add(key, entry);
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
          if (key !== undefined) {
            this.#indexA.add(key, entry);
          }
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
    if (bEntries === undefined || bEntries.length === 0) {
      const joinEntry = [
        makeJoinResult(
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
        makeJoinResult(
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
        // TODO(aa): This is a bug. We need to update the reference to the row
        // here, like:
        // existing[0] = joinEntry[0];
        // because otherwise when we retract/reassert later, we will send the
        // wrong version of the row.
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

    // There can be multiple entries for the same key just because of
    // remove/add in the same transaction. But also theoretically, there could
    // be multiple adds for the same key in the same transaction.
    const aEntries = this.#indexA.get(bKey);
    if (aEntries === undefined) {
      return [];
    }

    const ret: Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] = [];
    for (const [aRow, aMult] of aEntries) {
      const joinEntry = [
        makeJoinResult(
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

      // TODO(aa): This branch isn't necessary now, it's always correct to use
      // a.id. This was thinking ahead to compound keys.
      // See: https://rocicorp.slack.com/archives/C013XFG80JC/p1720724435728549
      const aPrimaryKey = isJoinResult(aRow)
        ? aRow.id
        : this.#getAPrimaryKey(aRow as AValue);

      // This is tricky -- can we do it differently?
      //
      // The problem is that if we get a left row, and there are no right rows,
      // then we will emit a join result with a left side and null for right
      // side. If a right side then comes in, we need to know to retract the
      // left side. The reason this doesn't work as it does in inner join is
      // that inner join doesn't have this problem because it doesn't emit the
      // left side if there's no right side. So if a new right side comes in,
      // it's an edit and we get the right side retraction which naturally
      // leads to the join retraction. Here in left join that doesn't happen
      // and we need to synthesize the join retraction somehow.
      //
      // TODO(aa): also explore first-class subqueries as a solution.
      const existing = this.#aMatches.get(aPrimaryKey);
      if (joinEntry[1] > 0 && existing && existing[1] === 0) {
        // Row `a` now has matches. Send the retraction for the join entry with
        // left side but no right side.
        ret.push([existing[0], -1]);
      } else if (
        joinEntry[1] < 0 &&
        existing &&
        existing[1] + joinEntry[1] === 0
      ) {
        // We went back to row `a` being an unmatch. Send the assertion for the
        // join entry with left.
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
