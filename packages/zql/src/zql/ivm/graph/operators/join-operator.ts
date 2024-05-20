import type {Selector} from '../../../ast/ast.js';
import {genCached, genConcat, genFlatMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import type {
  JoinResult,
  PipelineEntity,
  StringOrNumber,
  Version,
} from '../../types.js';
import {
  getPrimaryKeyValuesAsStringUnqualified,
  getValueFromEntity,
} from '../../source/util.js';
import type {DifferenceStream} from '../difference-stream.js';
import {combineRows, DifferenceIndex} from './difference-index.js';
import {JoinOperatorBase} from './join-operator-base.js';

export type JoinArgs<
  AValue extends PipelineEntity,
  BValue extends PipelineEntity,
  ATable extends string | undefined,
  BAlias extends string | undefined,
> = {
  a: DifferenceStream<AValue>;
  // a is currently un-aliasable in ZQL. Hence `aTable` not `aAlias`.
  // The value is `undefined` if the `a` stream is producing a `join` result.
  aTable: ATable | undefined;
  aPrimaryKeyColumns: readonly (keyof AValue & string)[];
  // join column is a selector since we could be joining a join result to another
  // join result.
  aJoinColumn: Selector;
  b: DifferenceStream<BValue>;
  // bTable is always defined at the moment.
  // Seems like this will not be true if the B input is ever a query rather than a table.
  bTable: string;
  bAs: BAlias;
  bPrimaryKeyColumns: readonly (keyof BValue & string)[];
  bJoinColumn: Selector;
  output: DifferenceStream<JoinResult<AValue, BValue, ATable, BAlias>>;
};

/**
 * Joins two streams.
 *
 * Inputs:
 * - Stream A of changes
 * - Stream B of changes
 * - A function to extract the key to join on from A
 * - A function to extract the key to join on from B
 * - A function to compare the two keys
 *
 * The output is a stream of joined values of the form:
 *
 * ```ts
 * {
 *   table_name_1_or_alias: row_from_t1,
 *   table_name_2_or_alias: row_from_t2,
 * }[]
 * ```
 *
 * From which the `select` operator can extract the desired fields.
 */
export class InnerJoinOperator<
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
  readonly #indexA: DifferenceIndex<StringOrNumber, AValue>;
  readonly #indexB: DifferenceIndex<StringOrNumber, BValue>;
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

  #aKeysForCompaction = new Set<StringOrNumber>();
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

    if (inputB !== undefined) {
      iterablesToReturn.push(
        genFlatMap(inputB, entry => {
          const key = this.#getBJoinKey(entry[0]);
          const ret = this.#joinOne(
            entry,
            key,
            this.#indexA,
            this.#joinArgs.bAs,
            this.#joinArgs.aTable,
            this.#getBPrimaryKey,
            this.#getAPrimaryKey,
          );
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
          const ret = this.#joinOne(
            entry,
            key,
            this.#indexB,
            this.#joinArgs.aTable,
            this.#joinArgs.bAs,
            this.#getAPrimaryKey,
            this.#getBPrimaryKey,
          );
          if (key !== undefined) {
            this.#indexA.add(key, entry);
            this.#aKeysForCompaction.add(key);
          }
          return ret;
        }),
      );
    }

    return genCached(genConcat(iterablesToReturn));
  }

  #joinOne<OuterValue, InnerValue>(
    outerEntry: Entry<OuterValue>,
    outerKey: StringOrNumber,
    innerIndex: DifferenceIndex<StringOrNumber, InnerValue>,
    outerAlias: string | undefined,
    innerAlias: string | undefined,
    getOuterValueIdentity: (value: OuterValue) => StringOrNumber,
    getInnerValueIdentity: (value: InnerValue) => StringOrNumber,
  ): Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] {
    const outerValue = outerEntry[0];
    const outerMult = outerEntry[1];

    if (outerKey === undefined) {
      return [];
    }

    const innerEtnries = innerIndex.get(outerKey);
    if (innerEtnries === undefined) {
      return [];
    }

    const ret: Entry<JoinResult<AValue, BValue, ATable, BAlias>>[] = [];
    for (const [innerValue, innerMult] of innerEtnries) {
      const value = combineRows(
        outerValue,
        innerValue,
        outerAlias,
        innerAlias,
        getOuterValueIdentity,
        getInnerValueIdentity,
      );

      ret.push([
        value as JoinResult<AValue, BValue, ATable, BAlias>,
        outerMult * innerMult,
      ] as const);
    }
    return ret;
  }

  toString() {
    return `indexa: ${this.#indexA.toString()}\n\n\nindexb: ${this.#indexB.toString()}`;
  }
}
