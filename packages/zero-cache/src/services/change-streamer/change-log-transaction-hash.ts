import {createHash, type Hash} from 'node:crypto';

export type ChangeLogHashRow = {
  readonly watermark: string;
  readonly pos: number;
  readonly tag: string;
  readonly change: string;
  readonly precommit: string | null;
};

/**
 * Computes a stable digest over the fields shared by the Postgres and SQLite
 * change logs. Values are length-prefixed so different field boundaries cannot
 * produce the same byte stream.
 */
export class ChangeLogTransactionHasher {
  readonly #hash: Hash;

  constructor() {
    this.#hash = createHash('sha256');
    writeString(this.#hash, 'zero-change-log-transaction-v1');
  }

  add(row: ChangeLogHashRow): void {
    writeString(this.#hash, row.watermark);
    writeString(this.#hash, String(row.pos));
    writeString(this.#hash, row.tag);
    writeString(this.#hash, row.change);
    if (row.precommit === null) {
      this.#hash.update(Uint8Array.of(0));
    } else {
      this.#hash.update(Uint8Array.of(1));
      writeString(this.#hash, row.precommit);
    }
  }

  digest(): string {
    return this.#hash.digest('hex');
  }
}

function writeString(hash: Hash, value: string): void {
  const bytes = Buffer.from(value);
  const length = Buffer.allocUnsafe(4);
  length.writeUInt32BE(bytes.byteLength);
  hash.update(length);
  hash.update(bytes);
}
