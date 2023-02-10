import type {Read, Value} from './store';

export const deleteSentinel = Symbol();
export type DeleteSentinel = typeof deleteSentinel;

export class WriteImplBase {
  protected readonly _pending: Map<string, Value | DeleteSentinel> = new Map();
  private readonly _read: Read;

  constructor(read: Read) {
    this._read = read;
  }

  async has(key: string): Promise<boolean> {
    switch (this._pending.get(key)) {
      case undefined:
        return this._read.has(key);
      case deleteSentinel:
        return false;
      default:
        return true;
    }
  }

  async get(key: string): Promise<Value | undefined> {
    const v = this._pending.get(key);
    switch (v) {
      case deleteSentinel:
        return undefined;
      case undefined:
        return this._read.get(key);
      default:
        return v;
    }
  }

  async put(key: string, value: Value): Promise<void> {
    this._pending.set(key, value);
  }

  async del(key: string): Promise<void> {
    this._pending.set(key, deleteSentinel);
  }

  release(): void {
    this._read.release();
  }

  get closed(): boolean {
    return this._read.closed;
  }
}
