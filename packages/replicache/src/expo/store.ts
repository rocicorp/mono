import type {SQLiteDatabaseManagerOptions} from '../kv/sqlite-store.ts';
import type {Read, Store, Write} from '../kv/store.ts';

export class ExpoStore implements Store {
  readonly #instance: Promise<Store>;
  #closed = false;

  constructor(
    name: string,
    opts?: Partial<Omit<SQLiteDatabaseManagerOptions, 'journalMode'>>,
  ) {
    this.#instance = this.#createInstance(name, opts);
  }

  async #createInstance(
    name: string,
    opts?: Partial<Omit<SQLiteDatabaseManagerOptions, 'journalMode'>>,
  ): Promise<Store> {
    const {create} = await import('./lazy.ts');
    return create(name, opts);
  }

  async read(): Promise<Read> {
    return (await this.#instance).read();
  }

  async write(): Promise<Write> {
    return (await this.#instance).write();
  }

  async close(): Promise<void> {
    await (await this.#instance).close();
    this.#closed = (await this.#instance).closed;
  }

  get closed(): boolean {
    return this.#closed;
  }
}

export async function dropExpoStore(name: string): Promise<void> {
  const {drop} = await import('./lazy.ts');
  return drop(name);
}
