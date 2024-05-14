import type {Primitive} from '../../../ast/ast.js';
import {genCached, genConcat, genFlatMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import type {JoinResult, StringOrNumber, Version} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import {combineRows, DifferenceIndex} from './difference-index.js';
import {JoinOperatorBase} from './join-operator-base.js';

export type JoinArgs<
  Key extends Primitive,
  AValue extends object,
  BValue extends object,
  AAlias extends string | undefined,
  BAlias extends string | undefined,
> = {
  a: DifferenceStream<AValue>;
  aAs: AAlias | undefined;
  getAJoinKey: (value: AValue) => Key | undefined;
  getAPrimaryKey: (value: AValue) => StringOrNumber;
  b: DifferenceStream<BValue>;
  bAs: BAlias | undefined;
  getBJoinKey: (value: BValue) => Key | undefined;
  getBPrimaryKey: (value: BValue) => StringOrNumber;
  output: DifferenceStream<JoinResult<AValue, BValue, AAlias, BAlias>>;
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
  K extends Primitive,
  AValue extends object,
  BValue extends object,
  AAlias extends string | undefined,
  BAlias extends string | undefined,
> extends JoinOperatorBase<
  AValue,
  BValue,
  // If AValue or BValue are join results
  // then they should be lifted and need no aliasing
  // since they're already aliased
  JoinResult<AValue, BValue, AAlias, BAlias>
> {
  readonly #indexA: DifferenceIndex<K, AValue>;
  readonly #indexB: DifferenceIndex<K, BValue>;
  readonly #joinArgs;

  constructor(joinArgs: JoinArgs<K, AValue, BValue, AAlias, BAlias>) {
    super(joinArgs.a, joinArgs.b, joinArgs.output, (version, inputA, inputB) =>
      this.#join(version, inputA, inputB),
    );
    this.#indexA = new DifferenceIndex<K, AValue>(joinArgs.getAPrimaryKey);
    this.#indexB = new DifferenceIndex<K, BValue>(joinArgs.getBPrimaryKey);
    this.#joinArgs = joinArgs;
  }

  #aKeysForCompaction = new Set<K>();
  #bKeysForCompaction = new Set<K>();
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
      JoinResult<AValue, BValue, AAlias, BAlias>
    >[] = [];

    if (inputA !== undefined) {
      iterablesToReturn.push(
        genFlatMap(inputA, entry => {
          const key = this.#joinArgs.getAJoinKey(entry[0]);
          const ret = this.#joinOne(
            entry,
            key,
            this.#indexB,
            this.#joinArgs.aAs,
            this.#joinArgs.bAs,
            this.#joinArgs.getAPrimaryKey,
            this.#joinArgs.getBPrimaryKey,
          );
          if (key !== undefined) {
            this.#indexA.add(key, entry);
            this.#aKeysForCompaction.add(key);
          }
          return ret;
        }),
      );
    }

    if (inputB !== undefined) {
      iterablesToReturn.push(
        genFlatMap(inputB, entry => {
          const key = this.#joinArgs.getBJoinKey(entry[0]);
          const ret = this.#joinOne(
            entry,
            key,
            this.#indexA,
            this.#joinArgs.bAs,
            this.#joinArgs.aAs,
            this.#joinArgs.getBPrimaryKey,
            this.#joinArgs.getAPrimaryKey,
          );
          if (key !== undefined) {
            this.#indexB.add(key, entry);
            this.#bKeysForCompaction.add(key);
          }
          return ret;
        }),
      );
    }

    return genCached(genConcat(iterablesToReturn));
  }

  #joinOne<OuterValue, InnerValue>(
    outerEntry: Entry<OuterValue>,
    outerKey: K | undefined,
    innerIndex: DifferenceIndex<K, InnerValue>,
    outerAlias: string | undefined,
    innerAlias: string | undefined,
    getOuterValueIdentity: (value: OuterValue) => StringOrNumber,
    getInnerValueIdentity: (value: InnerValue) => StringOrNumber,
  ): Entry<JoinResult<AValue, BValue, AAlias, BAlias>>[] {
    const outerValue = outerEntry[0];
    const outerMult = outerEntry[1];

    if (outerKey === undefined) {
      return [];
    }

    const innerEtnries = innerIndex.get(outerKey);
    if (innerEtnries === undefined) {
      return [];
    }

    const ret: Entry<JoinResult<AValue, BValue, AAlias, BAlias>>[] = [];
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
        value as JoinResult<AValue, BValue, AAlias, BAlias>,
        outerMult * innerMult,
      ] as const);
    }
    return ret;
  }

  toString() {
    return `indexa: ${this.#indexA.toString()}\n\n\nindexb: ${this.#indexB.toString()}`;
  }
}
