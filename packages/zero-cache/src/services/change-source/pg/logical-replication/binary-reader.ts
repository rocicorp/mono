// Forked from https://github.com/kibae/pg-logical-replication/blob/c55abddc62eadd61bd38922037ecb7a1469fa8c3/src/output-plugins/pgoutput/binary-reader.ts

// should not use { fatal: true } because ErrorResponse can use invalid utf8 chars
const textDecoder = new TextDecoder();

// https://www.postgresql.org/docs/14/protocol-message-types.html
export class BinaryReader {
  #p = 0;
  readonly #b: Uint8Array;

  constructor(b: Uint8Array) {
    this.#b = b;
  }

  readUint8() {
    this.checkSize(1);

    return this.#b[this.#p++];
  }

  readInt16() {
    this.checkSize(2);

    return (this.#b[this.#p++] << 8) | this.#b[this.#p++];
  }

  readInt32() {
    this.checkSize(4);

    return (
      (this.#b[this.#p++] << 24) |
      (this.#b[this.#p++] << 16) |
      (this.#b[this.#p++] << 8) |
      this.#b[this.#p++]
    );
  }

  readString() {
    const endIdx = this.#b.indexOf(0x00, this.#p);

    if (endIdx < 0) {
      // TODO PgError.protocol_violation
      throw Error('unexpected end of message');
    }

    const strBuf = this.#b.subarray(this.#p, endIdx);
    this.#p = endIdx + 1;

    return this.decodeText(strBuf);
  }

  decodeText(strBuf: Uint8Array) {
    return textDecoder.decode(strBuf);
  }

  read(n: number) {
    this.checkSize(n);

    return this.#b.subarray(this.#p, (this.#p += n));
  }

  checkSize(n: number) {
    if (this.#b.length < this.#p + n) {
      // TODO PgError.protocol_violation
      throw Error('unexpected end of message');
    }
  }

  array<T>(length: number, fn: () => T): T[] {
    return Array.from({length}, fn, this);
  }

  // replication helpers
  readLsn() {
    const h = this.readUint32();
    const l = this.readUint32();

    if (h === 0 && l === 0) {
      return null;
    }

    return `${h.toString(16).padStart(8, '0')}/${l
      .toString(16)
      .padStart(8, '0')}`.toUpperCase();
  }

  readTime() {
    // (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY == 946684800000000
    return this.readUint64() + BigInt('946684800000000');
  }

  readUint64() {
    return (
      (BigInt(this.readUint32()) << BigInt(32)) | BigInt(this.readUint32())
    );
  }

  readUint32() {
    return this.readInt32() >>> 0;
  }
}
