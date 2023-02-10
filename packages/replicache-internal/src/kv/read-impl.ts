import type {FrozenJSONValue} from '../json.js';
import type {Read} from './store.js';

export class ReadImpl implements Read {
  private readonly _map: Map<string, FrozenJSONValue>;
  private readonly _release: () => void;
  private _closed = false;

  constructor(map: Map<string, FrozenJSONValue>, release: () => void) {
    this._map = map;
    this._release = release;
  }

  release() {
    this._release();
    this._closed = true;
  }

  get closed(): boolean {
    return this._closed;
  }

  has(key: string): Promise<boolean> {
    return Promise.resolve(this._map.has(key));
  }

  get(key: string): Promise<FrozenJSONValue | undefined> {
    return Promise.resolve(this._map.get(key));
  }
}
