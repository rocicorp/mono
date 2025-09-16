/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import type {FrozenJSONValue} from '../frozen-json.ts';
import type {Read} from './store.ts';

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
