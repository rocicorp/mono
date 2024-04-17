import type {Primitive} from '../../../ast/ast.js';
import type {Entry, Multiset} from '../../multiset.js';
import type {JoinResult, StringOrNumber} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import {BinaryOperator} from './binary-operator.js';
import {DifferenceIndex, joinType} from './difference-index.js';

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
> extends BinaryOperator<
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
    super(joinArgs.a, joinArgs.b, joinArgs.output, (_, inputA, inputB) =>
      this.#join(inputA, inputB),
    );
    this.#indexA = new DifferenceIndex<K, AValue>(joinArgs.getAPrimaryKey);
    this.#indexB = new DifferenceIndex<K, BValue>(joinArgs.getBPrimaryKey);
    this.#joinArgs = joinArgs;
  }

  #join(
    inputA: Multiset<AValue> | undefined,
    inputB: Multiset<BValue> | undefined,
  ) {
    const {aAs, getAJoinKey, getAPrimaryKey, bAs, getBJoinKey, getBPrimaryKey} =
      this.#joinArgs;
    const aKeysForCompaction: K[] = [];
    const bKeysForCompaction: K[] = [];
    const deltaA = new DifferenceIndex<K, AValue>(getAPrimaryKey);
    for (const entry of inputA || []) {
      const aKey = getAJoinKey(entry[0]);
      if (aKey === undefined) {
        continue;
      }
      deltaA.add(aKey, entry);
      aKeysForCompaction.push(aKey);
    }

    const deltaB = new DifferenceIndex<K, BValue>(getBPrimaryKey);
    for (const entry of inputB || []) {
      const bKey = getBJoinKey(entry[0]);
      if (bKey === undefined) {
        continue;
      }
      deltaB.add(bKey, entry);
      bKeysForCompaction.push(bKey);
    }

    const result: Entry<JoinResult<AValue, BValue, AAlias, BAlias>>[] = [];
    // TODO: just concat the two iterables rather than pushing onto result
    for (const x of deltaA.join(
      joinType.inner,
      aAs,
      this.#indexB,
      bAs,
      getBPrimaryKey,
    )[0]) {
      result.push(x);
    }
    this.#indexA.extend(deltaA);

    // TODO: just concat the two iterables rather than pushing onto result
    for (const x of deltaB.join(
      joinType.inner,
      bAs,
      this.#indexA,
      aAs,
      getAPrimaryKey,
    )[0]) {
      result.push(x);
    }
    this.#indexB.extend(deltaB);

    this.#indexA.compact(aKeysForCompaction);
    this.#indexB.compact(bKeysForCompaction);
    return result;
  }

  toString() {
    return `indexa: ${this.#indexA.toString()}\n\n\nindexb: ${this.#indexB.toString()}`;
  }
}
