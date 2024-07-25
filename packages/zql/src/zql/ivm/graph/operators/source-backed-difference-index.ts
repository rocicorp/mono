import type {Primitive} from '../../../ast/ast.js';
import type {Entry} from '../../multiset.js';
import type {SourceHashIndex} from '../../source/source-hash-index.js';
import type {PipelineEntity} from '../../types.js';
import type {DifferenceIndex} from './difference-index.js';

export class SourceBackedDifferenceIndex<
  Key extends Primitive,
  V extends PipelineEntity,
> implements DifferenceIndex<Key, V>
{
  readonly #overlayIndex: Map<Key, Entry<V>[]>;
  readonly #sourceIndex: SourceHashIndex<Key, V>;

  constructor(sourceIndex: SourceHashIndex<Key, V>) {
    this.#overlayIndex = new Map();
    this.#sourceIndex = sourceIndex;
  }

  add(key: Key, entry: Entry<V>) {
    const mult = entry[1];

    if (mult > 0) {
      // already present in the source
      return;
    }

    let existing = this.#overlayIndex.get(key);
    if (existing === undefined) {
      existing = [];
      this.#overlayIndex.set(key, existing);
    }
    existing.push(entry);
    existing.push([entry[0], -mult]);
  }

  get(key: Key): Entry<V>[] {
    const overlayResult = this.#overlayIndex.get(key) ?? [];
    const sourceResult = this.#sourceIndex.get(key) ?? [];
    const ret: Entry<V>[] = [];
    for (const value of sourceResult) {
      ret.push([value, 1]);
    }
    ret.push(...overlayResult);
    return ret;
  }

  compact() {
    this.#overlayIndex.clear();
  }

  trackKeyForCompaction(_key: Key) {}
}
