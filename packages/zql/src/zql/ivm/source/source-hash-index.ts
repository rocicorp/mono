import type {Primitive, Selector} from '../../ast/ast.js';
import type {PipelineEntity} from '../types.js';
import {getValueFromEntity} from './util.js';

export class SourceHashIndex<K extends Primitive, T extends PipelineEntity> {
  readonly #column;
  readonly #index = new Map<K, T[]>();

  constructor(column: Selector) {
    this.#column = column;
  }

  get(key: K): T[] | undefined {
    return this.#index.get(key);
  }

  add(value: T): this {
    const key = getValueFromEntity(value, this.#column) as K;
    let existing = this.#index.get(key);
    if (existing === undefined) {
      existing = [];
      this.#index.set(key, existing);
    }
    existing.push(value);
    return this;
  }

  delete(value: T): this {
    const key = getValueFromEntity(value, this.#column) as K;
    const existing = this.#index.get(key);
    if (existing === undefined) {
      return this;
    }
    const index = existing.indexOf(value);
    if (index !== -1) {
      existing.splice(index, 1);
    }
    if (existing.length === 0) {
      this.#index.delete(key);
    }
    return this;
  }
}
