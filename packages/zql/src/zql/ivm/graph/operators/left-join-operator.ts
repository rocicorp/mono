import type {Primitive} from '../../../ast/ast.js';
import type {Entry, Multiset} from '../../multiset.js';
import {isJoinResult, JoinResult, StringOrNumber} from '../../types.js';
import {BinaryOperator} from './binary-operator.js';
import {DifferenceIndex, joinType} from './difference-index.js';
import type {JoinArgs} from './join-operator.js';

export class LeftJoinOperator<
  K extends Primitive,
  AValue extends object,
  BValue extends object,
  AAlias extends string | undefined,
  BAlias extends string | undefined,
> extends BinaryOperator<
  AValue,
  BValue,
  JoinResult<AValue, BValue, AAlias, BAlias>
> {
  readonly #indexA: DifferenceIndex<K | undefined, AValue>;
  readonly #indexB: DifferenceIndex<K, BValue>;
  readonly #joinArgs;
  readonly #aMatches: Map<
    StringOrNumber,
    [JoinResult<AValue, BValue, AAlias, BAlias>, number]
  > = new Map();

  constructor(joinArgs: JoinArgs<K, AValue, BValue, AAlias, BAlias>) {
    super(joinArgs.a, joinArgs.b, joinArgs.output, (_, inputA, inputB) =>
      this.#join(inputA, inputB),
    );
    this.#indexA = new DifferenceIndex<K | undefined, AValue>(
      joinArgs.getAPrimaryKey,
    );
    this.#indexB = new DifferenceIndex<K, BValue>(joinArgs.getBPrimaryKey);
    this.#joinArgs = joinArgs;
  }

  #join(
    inputA: Multiset<AValue> | undefined,
    inputB: Multiset<BValue> | undefined,
  ) {
    const {aAs, getAJoinKey, getAPrimaryKey, bAs, getBJoinKey, getBPrimaryKey} =
      this.#joinArgs;
    const aKeysForCompaction: (K | undefined)[] = [];
    const bKeysForCompaction: K[] = [];
    const deltaA = new DifferenceIndex<K | undefined, AValue>(getAPrimaryKey);

    for (const entry of inputA || []) {
      const aKey = getAJoinKey(entry[0]);
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
    let joinResult = deltaA.join(
      joinType.left,
      aAs,
      this.#indexB,
      bAs,
      getBPrimaryKey,
    );
    let joinMultiset = joinResult[0];
    let joinSourceRows = joinResult[1];
    let i = 0;
    for (const joinEntry of joinMultiset) {
      const bRow = joinSourceRows[i][1];
      const aRow = joinSourceRows[i][0];
      ++i;
      result.push(joinEntry);

      const aPrimaryKey = isJoinResult(aRow)
        ? aRow.id
        : getAPrimaryKey(aRow as AValue);
      if (bRow === undefined) {
        this.#aMatches.set(aPrimaryKey, [joinEntry[0], 0]);
      } else {
        const existing = this.#aMatches.get(aPrimaryKey);
        if (existing) {
          existing[1] += joinEntry[1];
        } else {
          this.#aMatches.set(aPrimaryKey, [joinEntry[0], joinEntry[1]]);
        }
      }
    }
    this.#indexA.extend(deltaA);

    joinResult = deltaB.join(
      joinType.inner,
      bAs,
      this.#indexA,
      aAs,
      getAPrimaryKey,
    );
    joinMultiset = joinResult[0];
    joinSourceRows = joinResult[1];
    i = 0;
    for (const joinEntry of joinMultiset) {
      result.push(joinEntry);

      // TODO: aRow should be an entry so we can apply proper mult
      const aRow = joinSourceRows[i][0];
      ++i;
      const aPrimaryKey = isJoinResult(aRow)
        ? aRow.id
        : getAPrimaryKey(aRow as AValue);

      // if we're adding a thing
      //  if we go from 0, retract unmatches aRow mult?
      // if we're removing a thing
      //  if we go to 0, send unmatches aRow mult?
      const existing = this.#aMatches.get(aPrimaryKey);
      if (joinEntry[1] > 0 && existing && existing[1] === 0) {
        // row `a` now has matches. Remove the un-match.
        result.push([existing[0], -1]);
      } else if (
        joinEntry[1] < 0 &&
        existing &&
        existing[1] + joinEntry[1] === 0
      ) {
        // We went back to row `a` being an unmatch. Send the un-match
        result.push([existing[0], 1]);
      }

      if (existing) {
        existing[1] += joinEntry[1];
      }
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
