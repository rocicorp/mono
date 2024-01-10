import type {FrozenJSONValue} from '../frozen-json.js';
import type {Read} from './store.js';

export class ReadImpl implements Read {
  readonly #map: Map<string, FrozenJSONValue>;
  readonly #release: () => void;
  #closed = false;

  constructor(map: Map<string, FrozenJSONValue>, release: () => void) {
    this.#map = map;
    this.#release = release;
  }

  release() {
    this.#release();
    this.#closed = true;
  }

  get closed(): boolean {
    return this.#closed;
  }

  has(key: string): Promise<boolean> {
    return Promise.resolve(this.#map.has(key));
  }

  get(key: string): Promise<FrozenJSONValue | undefined> {
    return Promise.resolve(this.#map.get(key));
  }
}
