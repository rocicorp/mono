import {describe, expect, test} from 'vitest';
import type {TypeParsers} from '../../../../db/pg-type-parser.ts';
import {
  BOOL,
  BYTEA,
  DATE,
  FLOAT4,
  FLOAT8,
  INT2,
  INT4,
  INT8,
  JSON as JSON_OID,
  JSONB,
  NUMERIC,
  TEXT,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  UUID,
  VARCHAR,
} from '../../../../types/pg-types.ts';
import {createStaticBinaryDecodersInstance} from './pgoutput-binary-decoders.ts';
import {PgoutputParser} from './pgoutput-parser.ts';
import type {
  MessageInsert,
  MessageRelation,
  MessageUpdate,
} from './pgoutput.types.ts';

function createDummyTypeParsers(): TypeParsers {
  return {
    getTypeParser: () => (val: string) => val,
  };
}

class BufferWriter {
  #chunks: Buffer[] = [];

  writeUint8(val: number) {
    const buf = Buffer.alloc(1);
    buf.writeUInt8(val, 0);
    this.#chunks.push(buf);
    return this;
  }

  writeInt16(val: number) {
    const buf = Buffer.alloc(2);
    buf.writeInt16BE(val, 0);
    this.#chunks.push(buf);
    return this;
  }

  writeInt32(val: number) {
    const buf = Buffer.alloc(4);
    buf.writeInt32BE(val, 0);
    this.#chunks.push(buf);
    return this;
  }

  writeString(str: string) {
    this.#chunks.push(Buffer.from(str, 'utf8'));
    this.writeUint8(0); // null terminator
    return this;
  }

  writeBytes(buf: Buffer) {
    this.#chunks.push(buf);
    return this;
  }

  toBuffer(): Buffer {
    return Buffer.concat(this.#chunks);
  }
}

function buildRelationMessage(
  relationOid: number,
  tableName: string,
  columns: {name: string; typeOid: number; isKey?: boolean}[],
): Buffer {
  const w = new BufferWriter();
  w.writeUint8(0x52); // 'R'
  w.writeInt32(relationOid);
  w.writeString('public');
  w.writeString(tableName);
  w.writeUint8(0x64); // 'd' default replica identity
  w.writeInt16(columns.length);

  for (const col of columns) {
    w.writeUint8(col.isKey ? 1 : 0);
    w.writeString(col.name);
    w.writeInt32(col.typeOid);
    w.writeInt32(-1); // typeMod
  }

  return w.toBuffer();
}

type TupleValue =
  | {kind: 'b'; val: Buffer}
  | {kind: 't'; val: string}
  | {kind: 'n'}
  | {kind: 'u'};

function buildInsertMessage(relationOid: number, values: TupleValue[]): Buffer {
  const w = new BufferWriter();
  w.writeUint8(0x49); // 'I'
  w.writeInt32(relationOid);
  w.writeUint8(0x4e); // 'N'
  w.writeInt16(values.length);

  for (const v of values) {
    switch (v.kind) {
      case 'b':
        w.writeUint8(0x62); // 'b'
        w.writeInt32(v.val.length);
        w.writeBytes(v.val);
        break;
      case 't': {
        w.writeUint8(0x74); // 't'
        const buf = Buffer.from(v.val, 'utf8');
        w.writeInt32(buf.length);
        w.writeBytes(buf);
        break;
      }
      case 'n':
        w.writeUint8(0x6e); // 'n'
        break;
      case 'u':
        w.writeUint8(0x75); // 'u'
        break;
    }
  }

  return w.toBuffer();
}

describe('PgoutputParser with binary decoding', () => {
  const binaryDecoders = createStaticBinaryDecodersInstance();
  const typeParsers = createDummyTypeParsers();

  test('parses binary values for all scalar types', () => {
    const parser = new PgoutputParser(typeParsers, binaryDecoders);

    // Register relation
    const relBuf = buildRelationMessage(1001, 'test_table', [
      {name: 'id', typeOid: TEXT, isKey: true},
      {name: 'b_true', typeOid: BOOL},
      {name: 'b_false', typeOid: BOOL},
      {name: 'i2', typeOid: INT2},
      {name: 'i4', typeOid: INT4},
      {name: 'i8', typeOid: INT8},
      {name: 'f4', typeOid: FLOAT4},
      {name: 'f8', typeOid: FLOAT8},
      {name: 'vc', typeOid: VARCHAR},
      {name: 'u', typeOid: UUID},
      {name: 'ba', typeOid: BYTEA},
      {name: 'j', typeOid: JSON_OID},
      {name: 'jb', typeOid: JSONB},
      {name: 'ts', typeOid: TIMESTAMP},
      {name: 'tstz', typeOid: TIMESTAMPTZ},
      {name: 'd', typeOid: DATE},
      {name: 't', typeOid: TIME},
      {name: 'num', typeOid: NUMERIC},
    ]);
    const relMsg = parser.parse(relBuf) as MessageRelation;
    expect(relMsg.tag).toBe('relation');
    expect(relMsg.name).toBe('test_table');

    // Encode binary values
    const bTrue = Buffer.from([1]);
    const bFalse = Buffer.from([0]);

    const i2Buf = Buffer.alloc(2);
    i2Buf.writeInt16BE(32767, 0);

    const i4Buf = Buffer.alloc(4);
    i4Buf.writeInt32BE(12345678, 0);

    const i8Buf = Buffer.alloc(8);
    i8Buf.writeBigInt64BE(9007199254740993n, 0);

    const f4Buf = Buffer.alloc(4);
    f4Buf.writeFloatBE(12.5, 0);

    const f8Buf = Buffer.alloc(8);
    f8Buf.writeDoubleBE(12345.6789, 0);

    const vcBuf = Buffer.from('hello varchar', 'utf8');

    // UUID: 16 bytes: 01234567-89ab-cdef-0123-456789abcdef
    const uuidBuf = Buffer.from('0123456789abcdef0123456789abcdef', 'hex');

    // BYTEA: 3 bytes: 0x01, 0x02, 0x03
    const byteaBuf = Buffer.from([1, 2, 3]);

    // JSON: {"k":"v"}
    const jsonBuf = Buffer.from('{"k":"v"}', 'utf8');

    // JSONB: 0x01 (version 1) followed by {"k":"v"}
    const jsonbBuf = Buffer.concat([
      Buffer.from([1]),
      Buffer.from('{"k":"v"}', 'utf8'),
    ]);

    // TIMESTAMP: 8 bytes microseconds since 2000-01-01 00:00:00 UTC (PG epoch = 946684800000 ms)
    // 0 ms offset from PG epoch = 2000-01-01T00:00:00.000Z
    const tsBuf = Buffer.alloc(8);
    tsBuf.writeBigInt64BE(0n, 0);

    // TIMESTAMPTZ: 1_000_000 microseconds (1 second) past PG epoch
    const tstzBuf = Buffer.alloc(8);
    tstzBuf.writeBigInt64BE(1_000_000n, 0);

    // DATE: 4 bytes days since 2000-01-01. 0 = 2000-01-01
    const dateBuf = Buffer.alloc(4);
    dateBuf.writeInt32BE(0, 0);

    // TIME: 8 bytes microseconds since midnight. 3600_000_000 micros = 1 hour = 3,600,000 ms
    const timeBuf = Buffer.alloc(8);
    timeBuf.writeBigInt64BE(3_600_000_000n, 0);

    // NUMERIC: 12.34
    // ndigits=2, weight=0, sign=0x0000, dscale=2, digits=[12, 3400]
    const numBuf = Buffer.alloc(12);
    numBuf.writeInt16BE(2, 0); // ndigits
    numBuf.writeInt16BE(0, 2); // weight
    numBuf.writeInt16BE(0, 4); // sign (0 = positive)
    numBuf.writeInt16BE(2, 6); // dscale
    numBuf.writeInt16BE(12, 8); // digit 0
    numBuf.writeInt16BE(3400, 10); // digit 1

    const insertBuf = buildInsertMessage(1001, [
      {kind: 'b', val: Buffer.from('row-1', 'utf8')},
      {kind: 'b', val: bTrue},
      {kind: 'b', val: bFalse},
      {kind: 'b', val: i2Buf},
      {kind: 'b', val: i4Buf},
      {kind: 'b', val: i8Buf},
      {kind: 'b', val: f4Buf},
      {kind: 'b', val: f8Buf},
      {kind: 'b', val: vcBuf},
      {kind: 'b', val: uuidBuf},
      {kind: 'b', val: byteaBuf},
      {kind: 'b', val: jsonBuf},
      {kind: 'b', val: jsonbBuf},
      {kind: 'b', val: tsBuf},
      {kind: 'b', val: tstzBuf},
      {kind: 'b', val: dateBuf},
      {kind: 'b', val: timeBuf},
      {kind: 'b', val: numBuf},
    ]);

    const insertMsg = parser.parse(insertBuf) as MessageInsert;
    expect(insertMsg.tag).toBe('insert');
    expect(insertMsg.new).toEqual({
      id: 'row-1',
      b_true: true,
      b_false: false,
      i2: 32767,
      i4: 12345678,
      i8: 9007199254740993n,
      f4: 12.5,
      f8: 12345.6789,
      vc: 'hello varchar',
      u: '01234567-89ab-cdef-0123-456789abcdef',
      ba: new Uint8Array([1, 2, 3]),
      j: '{"k":"v"}',
      jb: '{"k":"v"}',
      ts: 946684800000,
      tstz: 946684801000,
      d: 946684800000,
      t: 3600000,
      num: 12.34,
    });
  });

  test('handles nulls, unchanged toast, and mixed text/binary attributes', () => {
    const parser = new PgoutputParser(typeParsers, binaryDecoders);

    const relBuf = buildRelationMessage(1002, 'mixed_table', [
      {name: 'id', typeOid: TEXT, isKey: true},
      {name: 'bin_col', typeOid: INT4},
      {name: 'txt_col', typeOid: TEXT},
      {name: 'null_col', typeOid: INT4},
      {name: 'toast_col', typeOid: TEXT},
    ]);
    parser.parse(relBuf);

    const i4Buf = Buffer.alloc(4);
    i4Buf.writeInt32BE(42, 0);

    // Initial insert
    const insertBuf = buildInsertMessage(1002, [
      {kind: 'b', val: Buffer.from('key-1', 'utf8')},
      {kind: 'b', val: i4Buf},
      {kind: 't', val: 'text-value'},
      {kind: 'n'},
      {kind: 'b', val: Buffer.from('large-toast-content', 'utf8')},
    ]);

    const insertMsg = parser.parse(insertBuf) as MessageInsert;
    expect(insertMsg.new).toEqual({
      id: 'key-1',
      bin_col: 42,
      txt_col: 'text-value',
      null_col: null,
      toast_col: 'large-toast-content',
    });

    // Update with unchanged TOAST datum ('u')
    const w = new BufferWriter();
    w.writeUint8(0x55); // 'U'
    w.writeInt32(1002);
    w.writeUint8(0x4f); // 'O' old tuple followed by 'N' new tuple
    // Old tuple (5 fields)
    w.writeInt16(5);
    w.writeUint8(0x62);
    w.writeInt32(5);
    w.writeBytes(Buffer.from('key-1', 'utf8'));
    w.writeUint8(0x62);
    w.writeInt32(4);
    w.writeBytes(i4Buf);
    w.writeUint8(0x74);
    w.writeInt32(10);
    w.writeBytes(Buffer.from('text-value', 'utf8'));
    w.writeUint8(0x6e);
    w.writeUint8(0x62);
    w.writeInt32(19);
    w.writeBytes(Buffer.from('large-toast-content', 'utf8'));

    // New tuple with unchanged TOAST ('u')
    w.writeUint8(0x4e); // 'N'
    w.writeInt16(5);
    w.writeUint8(0x62);
    w.writeInt32(5);
    w.writeBytes(Buffer.from('key-1', 'utf8'));
    const i4BufUpdated = Buffer.alloc(4);
    i4BufUpdated.writeInt32BE(99, 0);
    w.writeUint8(0x62);
    w.writeInt32(4);
    w.writeBytes(i4BufUpdated);
    w.writeUint8(0x74);
    w.writeInt32(11);
    w.writeBytes(Buffer.from('text-update', 'utf8'));
    w.writeUint8(0x6e);
    w.writeUint8(0x75); // 'u' unchanged toast!

    const updateMsg = parser.parse(w.toBuffer()) as MessageUpdate;
    expect(updateMsg.tag).toBe('update');
    expect(updateMsg.new).toEqual({
      id: 'key-1',
      bin_col: 99,
      txt_col: 'text-update',
      null_col: null,
      toast_col: 'large-toast-content', // Preserved from old tuple!
    });
  });

  test('parses binary arrays into native JS arrays', () => {
    const parser = new PgoutputParser(typeParsers, binaryDecoders);

    // INT4 array OID is 1007
    const relBuf = buildRelationMessage(1003, 'arr_table', [
      {name: 'id', typeOid: TEXT, isKey: true},
      {name: 'ints', typeOid: 1007},
    ]);
    parser.parse(relBuf);

    // Build binary array for [10, 20, 30]
    // Header: ndim(4), flags(4), elemOid(4)
    // Dim 0: len(4), lbound(4)
    // Elements: len(4) + val(4) for each
    const arrBuf = Buffer.alloc(12 + 8 + 3 * 8);
    let offset = 0;
    arrBuf.writeInt32BE(1, offset); // ndim = 1
    offset += 4;
    arrBuf.writeInt32BE(0, offset); // flags = 0
    offset += 4;
    arrBuf.writeInt32BE(INT4, offset); // elemOid = 23
    offset += 4;
    arrBuf.writeInt32BE(3, offset); // dim length = 3
    offset += 4;
    arrBuf.writeInt32BE(1, offset); // lower bound = 1
    offset += 4;

    for (const v of [10, 20, 30]) {
      arrBuf.writeInt32BE(4, offset); // elemLen = 4
      offset += 4;
      arrBuf.writeInt32BE(v, offset);
      offset += 4;
    }

    const insertBuf = buildInsertMessage(1003, [
      {kind: 'b', val: Buffer.from('arr-1', 'utf8')},
      {kind: 'b', val: arrBuf},
    ]);

    const insertMsg = parser.parse(insertBuf) as MessageInsert;
    expect(insertMsg.new).toEqual({
      id: 'arr-1',
      ints: [10, 20, 30],
    });
  });
});
