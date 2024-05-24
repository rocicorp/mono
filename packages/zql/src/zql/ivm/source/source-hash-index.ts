import type {Primitive, Selector} from '../../ast/ast.js';
import type {PipelineEntity} from '../types.js';
import {getValueFromEntity} from './util.js';

export class SourceHashIndex<K extends Primitive, T extends PipelineEntity> {
  readonly #column;
  readonly #index = new Map<K, Set<T>>();

  constructor(column: Selector) {
    this.#column = column;
  }

  get(key: K): Iterable<T> | undefined {
    return this.#index.get(key);
  }

  add(value: T): this {
    const key = getValueFromEntity(value, this.#column) as K;
    let existing = this.#index.get(key);
    if (existing === undefined) {
      existing = new Set();
      this.#index.set(key, existing);
    }
    existing.add(value);
    return this;
  }

  delete(value: T): this {
    const key = getValueFromEntity(value, this.#column) as K;
    const existing = this.#index.get(key);
    if (existing === undefined) {
      return this;
    }
    existing.delete(value);
    if (existing.size === 0) {
      this.#index.delete(key);
    }
    return this;
  }
}
