/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
// Forked from https://github.com/kibae/pg-logical-replication/blob/c55abddc62eadd61bd38922037ecb7a1469fa8c3/src/output-plugins/pgoutput/binary-reader.ts
/* eslint-disable */

// should not use { fatal: true } because ErrorResponse can use invalid utf8 chars
const textDecoder = new TextDecoder();

// https://www.postgresql.org/docs/14/protocol-message-types.html
export class BinaryReader {
  _p = 0;
  constructor(private _b: Uint8Array) {}

  readUint8() {
    this.checkSize(1);

    return this._b[this._p++];
  }

  readInt16() {
    this.checkSize(2);

    return (this._b[this._p++] << 8) | this._b[this._p++];
  }

  readInt32() {
    this.checkSize(4);

    return (
      (this._b[this._p++] << 24) |
      (this._b[this._p++] << 16) |
      (this._b[this._p++] << 8) |
      this._b[this._p++]
    );
  }

  readString() {
    const endIdx = this._b.indexOf(0x00, this._p);

    if (endIdx < 0) {
      // TODO PgError.protocol_violation
      throw Error('unexpected end of message');
    }

    const strBuf = this._b.subarray(this._p, endIdx);
    this._p = endIdx + 1;

    return this.decodeText(strBuf);
  }

  decodeText(strBuf: Uint8Array) {
    return textDecoder.decode(strBuf);
  }

  read(n: number) {
    this.checkSize(n);

    return this._b.subarray(this._p, (this._p += n));
  }

  checkSize(n: number) {
    if (this._b.length < this._p + n) {
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
