import {must} from 'shared/src/must.js';
import type {Selector} from '../../../ast/ast.js';
import {genCached, genConcat, genFlatMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import {
  getPrimaryKeyValuesAsStringUnqualified,
  getValueFromEntity,
} from '../../source/util.js';
import {
  isJoinResult,
  joinSymbol,
  type JoinResult,
  type PipelineEntity,
  type StringOrNumber,
  type Version,
} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import {MemoryBackedDifferenceIndex} from './difference-index.js';
import {JoinOperatorBase} from './join-operator-base.js';

export type JoinArgs<
  AValue extends PipelineEntity,
  BValue extends PipelineEntity,
  ATable extends string,
  BAlias extends string,
> = {
  a: DifferenceStream<AValue>;
  // a is currently un-aliasable in ZQL. Hence `aTable` not `aAlias`.
  // The value is `undefined` if the `a` stream is producing a `join` result.
  aTable: ATable | undefined;
  // TODO(aa); What is the pk column needed for?
  // Expecting only need to need the join key info as an arg.
  aPrimaryKeyColumns: readonly (keyof AValue & string)[];
  // join column is a selector since we could be joining a join result to another
  // join result.
  aJoinColumn: Selector;
  b: DifferenceStream<BValue>;
  // bTable is always defined at the moment.
  // Seems like this will not be true if the B input is ever a query rather than a table.
  bTable: string;
  bAs: BAlias | undefined;
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
  ATable extends string,
  BAlias extends string,
> extends JoinOperatorBase<
  AValue,
  BValue,
  // If AValue or BValue are join results
  // then they should be lifted and need no aliasing
  // since they're already aliased
  JoinResult<AValue, BValue, ATable, BAlias>
> {
  readonly #indexA: MemoryBackedDifferenceIndex<StringOrNumber, AValue>;
  readonly #indexB: MemoryBackedDifferenceIndex<StringOrNumber, BValue>;
  readonly #getAPrimaryKey;
  readonly #getBPrimaryKey;
  readonly #getAJoinKey;
  readonly #getBJoinKey;
  readonly #joinArgs: JoinArgs<AValue, BValue, ATable, BAlias>;

  constructor(joinArgs: JoinArgs<AValue, BValue, ATable, BAlias>) {
    super(
      joinArgs.a,
      joinArgs.b,
      joinArgs.output,
      (version, inputA, inputB) => this.#join(version, inputA, inputB),
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
      getValueFromEntity(value, joinArgs.aJoinColumn) as StringOrNumber;
    this.#getBJoinKey = (value: BValue) =>
      getValueFromEntity(value, joinArgs.bJoinColumn) as StringOrNumber;

    this.#indexA = new MemoryBackedDifferenceIndex<StringOrNumber, AValue>(
      this.#getAPrimaryKey,
    );
    this.#indexB = new MemoryBackedDifferenceIndex<StringOrNumber, BValue>(
      this.#getBPrimaryKey,
    );

    this.#joinArgs = joinArgs;
  }

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
      this.#indexA.compact();
      this.#indexB.compact();
      this.#lastVersion = version;
    }

    const iterablesToReturn: Multiset<
      JoinResult<AValue, BValue, ATable, BAlias>
    >[] = [];

    const {aTable} = this.#joinArgs;
    const bAs = must(this.#joinArgs.bAs);

    if (inputB !== undefined) {
      iterablesToReturn.push(
        genFlatMap(inputB, entry => {
          const key = this.#getBJoinKey(entry[0]);
          const ret = this.#joinOne(
            entry,
            key,
            this.#indexA,
            // Note: aVal and bVal swapped here in the callback params as
            // compared to the case below where inputA changes. This is because
            // we're joining B to A vs A to B. We still want to pass A first to
            // makeJoinResult though so that the generated ID for a given row is
            // consistent no matter which side of the join triggers it.
            (bVal, aVal) =>
              makeJoinResult(
                aVal,
                bVal,
                aTable,
                bAs,
                this.#getAPrimaryKey,
                this.#getBPrimaryKey,
              ),
          );
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
          const ret = this.#joinOne(entry, key, this.#indexB, (aVal, bVal) =>
            makeJoinResult(
              aVal,
              bVal,
              aTable,
              bAs,
              this.#getAPrimaryKey,
              this.#getBPrimaryKey,
            ),
          );
          if (key !== undefined) {
            this.#indexA.add(key, entry);
          }
          return ret;
        }),
      );
    }

    return genCached(genConcat(iterablesToReturn));
  }

  #joinOne<OuterValue, InnerValue, JoinResultValue>(
    outerEntry: Entry<OuterValue>,
    outerKey: StringOrNumber,
    innerIndex: MemoryBackedDifferenceIndex<StringOrNumber, InnerValue>,
    makeJoinResult: (a: OuterValue, b: InnerValue) => JoinResultValue,
  ): Entry<JoinResultValue>[] {
    const outerValue = outerEntry[0];
    const outerMult = outerEntry[1];

    if (outerKey === undefined) {
      return [];
    }

    const innerEntries = innerIndex.get(outerKey);
    if (innerEntries === undefined) {
      return [];
    }

    const ret: Entry<JoinResultValue>[] = [];
    for (const [innerValue, innerMult] of innerEntries) {
      const value = makeJoinResult(outerValue, innerValue);

      ret.push([value, outerMult * innerMult] as const);
    }
    return ret;
  }

  toString() {
    return `indexa: ${this.#indexA.toString()}\n\n\nindexb: ${this.#indexB.toString()}`;
  }
}

// We choose this particular separator because it's not part of common id
// formats like uuid or nanoid.
const joinIDSeparator = ':';

/**
 * We create IDs for JoinResults by concatenating the IDs of the two sides.
 * Because the input IDs for rows are arbitrary strings, we need to escape the
 * separator character to avoid ambiguity.
 */
export function encodeRowIDForJoin(id: string) {
  // We choose this particular encoding because it has fast impls in browsers
  // and because it passes uuid and nanoid through without encoding.
  return encodeURIComponent(id);
}

/**
 * Combine two input entries from a join into a JoinResult.
 *
 * Both sides of the join can already be JoinResults themselves, in which case
 * we want to flatten the structure. For example, if we join (a x b) x (c x d),
 * we want the result to be:
 *
 * { id: '...', a: aRow, b: bRow, c: cRow, d: dRow }
 *
 * Not:
 *
 * {
 *   id: '...',
 *   a_b: { a: aRow, b: bRow },
 *   c_d: { c: cRow, d: dRow }
 * }
 *
 * We also generate a new unique ID for the JoinResult by combining the IDs of
 * the input entries.
 */
export function makeJoinResult<
  AValue,
  BValue,
  AAlias extends string,
  BAlias extends string,
>(
  aVal: AValue,
  bVal: BValue | undefined,
  aAlias: AAlias | undefined,
  bAlias: BAlias | undefined,
  getAID: (value: AValue) => StringOrNumber,
  getBID: (value: BValue) => StringOrNumber,
): JoinResult<AValue, BValue, AAlias, BAlias> {
  const asJoinPart = (
    alias: string | undefined,
    id: StringOrNumber,
    val: AValue | BValue,
  ) => {
    if (isJoinResult(val)) {
      return val;
    }
    return {
      // It's OK to convert number to string here because ID types don't change
      // over the lifetime of a pipeline. So a number changing to string can't
      // cause a collision.
      id: encodeRowIDForJoin(id.toString()),
      [must(alias)]: val,
    } as const;
  };

  const aPart = asJoinPart(aAlias, getAID(aVal), aVal);
  const bPart = bVal ? asJoinPart(bAlias, getBID(bVal), bVal) : undefined;

  return {
    [joinSymbol]: true,
    ...aPart,
    ...bPart,
    id: `${aPart.id}${joinIDSeparator}${bPart?.id ?? ''}`,
  } as JoinResult<AValue, BValue, AAlias, BAlias>;
}
