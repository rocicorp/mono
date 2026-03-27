import {describe, expect, test} from 'vitest';
import {
  BinaryCopyParser,
  decodeDate,
  decodeNumeric,
  decodeTime,
  decodeTimeTZ,
  decodeTimestamp,
  decodeUUID,
  makeBinaryDecoder,
} from './pg-copy-binary.ts';

// Helper to build a valid PGCOPY binary header.
function pgcopyHeader(
  flags = 0,
  extensionData: Buffer = Buffer.alloc(0),
): Buffer {
  const signature = Buffer.from([
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
  ]);
  const flagsBuf = Buffer.alloc(4);
  flagsBuf.writeInt32BE(flags);
  const extLenBuf = Buffer.alloc(4);
  extLenBuf.writeInt32BE(extensionData.length);
  return Buffer.concat([signature, flagsBuf, extLenBuf, extensionData]);
}

// Helper to build a tuple with the given field values.
// null means SQL NULL, Buffer means raw field data.
function tuple(...fields: (Buffer | null)[]): Buffer {
  const fieldCountBuf = Buffer.alloc(2);
  fieldCountBuf.writeInt16BE(fields.length);
  const parts: Buffer[] = [fieldCountBuf];
  for (const field of fields) {
    const lenBuf = Buffer.alloc(4);
    if (field === null) {
      lenBuf.writeInt32BE(-1);
      parts.push(lenBuf);
    } else {
      lenBuf.writeInt32BE(field.length);
      parts.push(lenBuf, field);
    }
  }
  return Buffer.concat(parts);
}

// Trailer marker.
function trailer(): Buffer {
  const buf = Buffer.alloc(2);
  buf.writeInt16BE(-1);
  return buf;
}

// Helper to collect all parsed values from a parser.
function parseAll(
  parser: BinaryCopyParser,
  ...chunks: Buffer[]
): (Buffer | null)[] {
  const results: (Buffer | null)[] = [];
  for (const chunk of chunks) {
    for (const field of parser.parse(chunk)) {
      results.push(field);
    }
  }
  return results;
}

describe('BinaryCopyParser', () => {
  test('empty table (header + trailer only)', () => {
    const parser = new BinaryCopyParser();
    const data = Buffer.concat([pgcopyHeader(), trailer()]);
    const results = parseAll(parser, data);
    expect(results).toEqual([]);
  });

  test('single row with values', () => {
    const parser = new BinaryCopyParser();
    const int4Buf = Buffer.alloc(4);
    int4Buf.writeInt32BE(42);
    const textBuf = Buffer.from('hello', 'utf8');
    const data = Buffer.concat([
      pgcopyHeader(),
      tuple(int4Buf, textBuf),
      trailer(),
    ]);
    const results = parseAll(parser, data);
    expect(results).toHaveLength(2);
    expect(results[0]!.readInt32BE(0)).toBe(42);
    expect(results[1]!.toString('utf8')).toBe('hello');
  });

  test('NULL fields', () => {
    const parser = new BinaryCopyParser();
    const textBuf = Buffer.from('x', 'utf8');
    const data = Buffer.concat([
      pgcopyHeader(),
      tuple(null, textBuf, null),
      trailer(),
    ]);
    const results = parseAll(parser, data);
    expect(results).toEqual([null, expect.any(Buffer), null]);
    expect(results[1]!.toString('utf8')).toBe('x');
  });

  test('multiple rows', () => {
    const parser = new BinaryCopyParser();
    const val1 = Buffer.alloc(4);
    val1.writeInt32BE(1);
    const val2 = Buffer.alloc(4);
    val2.writeInt32BE(2);
    const data = Buffer.concat([
      pgcopyHeader(),
      tuple(val1),
      tuple(val2),
      trailer(),
    ]);
    const results = parseAll(parser, data);
    expect(results).toHaveLength(2);
    expect(results[0]!.readInt32BE(0)).toBe(1);
    expect(results[1]!.readInt32BE(0)).toBe(2);
  });

  test('chunked: header split across chunks', () => {
    const parser = new BinaryCopyParser();
    const val = Buffer.from('abc', 'utf8');
    const full = Buffer.concat([pgcopyHeader(), tuple(val), trailer()]);
    // Split in the middle of the header signature.
    const results = parseAll(parser, full.subarray(0, 5), full.subarray(5));
    expect(results).toHaveLength(1);
    expect(results[0]!.toString('utf8')).toBe('abc');
  });

  test('chunked: tuple header split across chunks', () => {
    const parser = new BinaryCopyParser();
    const val = Buffer.alloc(4);
    val.writeInt32BE(99);
    const full = Buffer.concat([pgcopyHeader(), tuple(val), trailer()]);
    const headerLen = pgcopyHeader().length;
    // Split 1 byte into the tuple header (which is 2 bytes for field count).
    const results = parseAll(
      parser,
      full.subarray(0, headerLen + 1),
      full.subarray(headerLen + 1),
    );
    expect(results).toHaveLength(1);
    expect(results[0]!.readInt32BE(0)).toBe(99);
  });

  test('chunked: field length split across chunks', () => {
    const parser = new BinaryCopyParser();
    const val = Buffer.from('test', 'utf8');
    const full = Buffer.concat([pgcopyHeader(), tuple(val), trailer()]);
    const headerLen = pgcopyHeader().length;
    // Split 4 bytes into: 2 (field count) + 2 (partial field length).
    const splitAt = headerLen + 4;
    const results = parseAll(
      parser,
      full.subarray(0, splitAt),
      full.subarray(splitAt),
    );
    expect(results).toHaveLength(1);
    expect(results[0]!.toString('utf8')).toBe('test');
  });

  test('chunked: field data split across chunks', () => {
    const parser = new BinaryCopyParser();
    const val = Buffer.from('hello world', 'utf8');
    const full = Buffer.concat([pgcopyHeader(), tuple(val), trailer()]);
    const headerLen = pgcopyHeader().length;
    // Split in the middle of field data: header + 2 (field count) + 4 (length) + 3 (partial data)
    const splitAt = headerLen + 2 + 4 + 3;
    const results = parseAll(
      parser,
      full.subarray(0, splitAt),
      full.subarray(splitAt),
    );
    expect(results).toHaveLength(1);
    expect(results[0]!.toString('utf8')).toBe('hello world');
  });

  test('chunked: one byte at a time', () => {
    const parser = new BinaryCopyParser();
    const val = Buffer.from('xy', 'utf8');
    const full = Buffer.concat([pgcopyHeader(), tuple(val, null), trailer()]);
    const chunks = Array.from(full).map(b => Buffer.from([b]));
    const results = parseAll(parser, ...chunks);
    expect(results).toHaveLength(2);
    expect(results[0]!.toString('utf8')).toBe('xy');
    expect(results[1]).toBeNull();
  });

  test('rejects invalid signature', () => {
    const parser = new BinaryCopyParser();
    const bad = Buffer.alloc(19);
    bad.write('NOT_PGCOPY');
    expect(() => parseAll(parser, bad)).toThrow(
      'Invalid PGCOPY binary signature',
    );
  });

  test('rejects nonzero flags', () => {
    const parser = new BinaryCopyParser();
    const data = pgcopyHeader(1);
    expect(() => parseAll(parser, data)).toThrow('Unsupported PGCOPY flags');
  });

  test('header with extension data', () => {
    const parser = new BinaryCopyParser();
    const extData = Buffer.from('extension-data');
    const val = Buffer.from('ok', 'utf8');
    const data = Buffer.concat([
      pgcopyHeader(0, extData),
      tuple(val),
      trailer(),
    ]);
    const results = parseAll(parser, data);
    expect(results).toHaveLength(1);
    expect(results[0]!.toString('utf8')).toBe('ok');
  });

  test('empty fields (zero-length)', () => {
    const parser = new BinaryCopyParser();
    const emptyBuf = Buffer.alloc(0);
    const data = Buffer.concat([pgcopyHeader(), tuple(emptyBuf), trailer()]);
    const results = parseAll(parser, data);
    expect(results).toHaveLength(1);
    expect(results[0]!.length).toBe(0);
  });
});

describe('binary decoders', () => {
  test('decodeUUID', () => {
    // UUID: 550e8400-e29b-41d4-a716-446655440000
    const buf = Buffer.from('550e8400e29b41d4a716446655440000', 'hex');
    expect(decodeUUID(buf)).toBe('550e8400-e29b-41d4-a716-446655440000');
  });

  test('decodeTimestamp', () => {
    // Work backward from a known Unix timestamp to PG microseconds.
    const expectedMs = new Date('2024-01-15T12:30:00Z').getTime();
    const pgMicroseconds = BigInt(expectedMs - 946_684_800_000) * 1000n;
    const buf = Buffer.alloc(8);
    buf.writeBigInt64BE(pgMicroseconds);
    expect(decodeTimestamp(buf)).toBe(expectedMs);
  });

  test('decodeTimestamp with sub-millisecond precision', () => {
    // 500 microseconds after PG epoch
    const buf = Buffer.alloc(8);
    buf.writeBigInt64BE(500n); // 0.5ms
    const result = decodeTimestamp(buf);
    // PG_EPOCH_UNIX_MILLIS + 0 whole ms + 0.5 remainder ms
    expect(result).toBeCloseTo(946_684_800_000 + 0.5, 3);
  });

  test('decodeDate', () => {
    // Work backward from a known date to PG days.
    const expected = Date.UTC(2024, 0, 15);
    const pgDays = expected / 86_400_000 - 10_957; // Unix days - PG epoch offset
    const buf = Buffer.alloc(4);
    buf.writeInt32BE(pgDays);
    expect(decodeDate(buf)).toBe(expected);
  });

  test('decodeDate epoch', () => {
    // 2000-01-01 = 0 days after PG epoch
    const buf = Buffer.alloc(4);
    buf.writeInt32BE(0);
    const result = decodeDate(buf);
    const expected = Date.UTC(2000, 0, 1);
    expect(result).toBe(expected);
  });

  test('decodeDate before PG epoch', () => {
    // 1999-12-31 = -1 days from PG epoch
    const buf = Buffer.alloc(4);
    buf.writeInt32BE(-1);
    const result = decodeDate(buf);
    const expected = Date.UTC(1999, 11, 31);
    expect(result).toBe(expected);
  });

  test('decodeTime', () => {
    // 12:30:00 = 45000 seconds = 45000000000 microseconds
    const buf = Buffer.alloc(8);
    buf.writeBigInt64BE(45_000_000_000n);
    const result = decodeTime(buf);
    expect(result).toBe(45_000_000); // 45000 seconds in ms
  });

  test('decodeTime midnight', () => {
    const buf = Buffer.alloc(8);
    buf.writeBigInt64BE(0n);
    expect(decodeTime(buf)).toBe(0);
  });

  test('decodeTimeTZ', () => {
    // 12:30:00 local with UTC+0 offset (offset=0 means UTC in PG)
    const buf = Buffer.alloc(12);
    buf.writeBigInt64BE(45_000_000_000n); // 12:30:00 in microseconds
    buf.writeInt32BE(0, 8); // UTC offset = 0
    const result = decodeTimeTZ(buf);
    expect(result).toBe(45_000_000);
  });

  test('decodeNumeric positive', () => {
    // 12345.67
    // In base-10000: weight=1, digits=[1, 2345, 6700]
    // 1 * 10000^1 + 2345 * 10000^0 + 6700 * 10000^-1 = 10000 + 2345 + 0.67 = 12345.67
    const buf = Buffer.alloc(8 + 3 * 2); // header (8) + 3 digits
    buf.writeInt16BE(3, 0); // ndigits
    buf.writeInt16BE(1, 2); // weight
    buf.writeUInt16BE(0, 4); // sign = positive
    buf.writeInt16BE(2, 6); // dscale
    buf.writeInt16BE(1, 8); // digit[0]
    buf.writeInt16BE(2345, 10); // digit[1]
    buf.writeInt16BE(6700, 12); // digit[2]
    expect(decodeNumeric(buf)).toBeCloseTo(12345.67, 2);
  });

  test('decodeNumeric negative', () => {
    const buf = Buffer.alloc(8 + 1 * 2);
    buf.writeInt16BE(1, 0); // ndigits
    buf.writeInt16BE(0, 2); // weight
    buf.writeUInt16BE(0x4000, 4); // sign = negative
    buf.writeInt16BE(0, 6); // dscale
    buf.writeInt16BE(42, 8); // digit[0]
    expect(decodeNumeric(buf)).toBe(-42);
  });

  test('decodeNumeric zero', () => {
    const buf = Buffer.alloc(8);
    buf.writeInt16BE(0, 0); // ndigits = 0
    buf.writeInt16BE(0, 2);
    buf.writeUInt16BE(0, 4);
    buf.writeInt16BE(0, 6);
    expect(decodeNumeric(buf)).toBe(0);
  });

  test('decodeNumeric NaN', () => {
    const buf = Buffer.alloc(8);
    buf.writeInt16BE(0, 0);
    buf.writeInt16BE(0, 2);
    buf.writeUInt16BE(0xc000, 4); // NaN
    buf.writeInt16BE(0, 6);
    expect(decodeNumeric(buf)).toBeNaN();
  });
});

describe('makeBinaryDecoder', () => {
  test('BOOL decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 16, dataType: 'bool'});
    expect(decode(Buffer.from([1]))).toBe(1);
    expect(decode(Buffer.from([0]))).toBe(0);
  });

  test('INT2 decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 21, dataType: 'int2'});
    const buf = Buffer.alloc(2);
    buf.writeInt16BE(-123);
    expect(decode(buf)).toBe(-123);
  });

  test('INT4 decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 23, dataType: 'int4'});
    const buf = Buffer.alloc(4);
    buf.writeInt32BE(2_000_000);
    expect(decode(buf)).toBe(2_000_000);
  });

  test('INT8 decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 20, dataType: 'int8'});
    const buf = Buffer.alloc(8);
    buf.writeBigInt64BE(9_007_199_254_740_993n);
    expect(decode(buf)).toBe(9_007_199_254_740_993n);
  });

  test('FLOAT4 decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 700, dataType: 'float4'});
    const buf = Buffer.alloc(4);
    buf.writeFloatBE(3.14);
    expect(decode(buf)).toBeCloseTo(3.14, 2);
  });

  test('FLOAT8 decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 701, dataType: 'float8'});
    const buf = Buffer.alloc(8);
    buf.writeDoubleBE(3.141592653589793);
    expect(decode(buf)).toBe(3.141592653589793);
  });

  test('TEXT decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 25, dataType: 'text'});
    expect(decode(Buffer.from('hello world'))).toBe('hello world');
  });

  test('UUID decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 2950, dataType: 'uuid'});
    const buf = Buffer.from('a0eebc999c0b4ef8bb6d6bb9bd380a11', 'hex');
    expect(decode(buf)).toBe('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
  });

  test('JSON decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 114, dataType: 'json'});
    expect(decode(Buffer.from('{"key":"value"}'))).toBe('{"key":"value"}');
  });

  test('JSONB decoder (skips version byte)', () => {
    const decode = makeBinaryDecoder({typeOID: 3802, dataType: 'jsonb'});
    const buf = Buffer.concat([Buffer.from([0x01]), Buffer.from('{"k":1}')]);
    expect(decode(buf)).toBe('{"k":1}');
  });

  test('BYTEA decoder', () => {
    const decode = makeBinaryDecoder({typeOID: 17, dataType: 'bytea'});
    const buf = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
    const result = decode(buf);
    expect(result).toBeInstanceOf(Uint8Array);
    expect(Array.from(result as Uint8Array)).toEqual([0xde, 0xad, 0xbe, 0xef]);
  });

  test('Enum decoder', () => {
    const decode = makeBinaryDecoder({
      typeOID: 99999,
      dataType: 'my_status',
      pgTypeClass: 'e',
    });
    expect(decode(Buffer.from('active'))).toBe('active');
  });

  test('fallback decoder (unknown type)', () => {
    const decode = makeBinaryDecoder({typeOID: 99999, dataType: 'unknown'});
    expect(decode(Buffer.from('raw text'))).toBe('raw text');
  });

  test('array decoder (int[])', () => {
    const decode = makeBinaryDecoder({
      typeOID: 1007,
      dataType: 'int4[]',
      elemPgTypeClass: 'b',
    });

    // Build a 1-dimensional int4 array: [10, 20, 30]
    const buf = Buffer.alloc(
      4 +
        4 +
        4 + // ndim, flags, elem_oid
        4 +
        4 + // dim[0] size + lower_bound
        3 * (4 + 4), // 3 elements, each with 4-byte length + 4-byte int4
    );
    let offset = 0;
    buf.writeInt32BE(1, offset);
    offset += 4; // ndim = 1
    buf.writeInt32BE(0, offset);
    offset += 4; // flags = 0
    buf.writeInt32BE(23, offset);
    offset += 4; // elem_oid = INT4
    buf.writeInt32BE(3, offset);
    offset += 4; // dim size = 3
    buf.writeInt32BE(1, offset);
    offset += 4; // lower bound = 1

    // Elements: 10, 20, 30
    for (const val of [10, 20, 30]) {
      buf.writeInt32BE(4, offset);
      offset += 4; // length = 4
      buf.writeInt32BE(val, offset);
      offset += 4; // value
    }

    const result = decode(buf);
    expect(result).toBe('[10,20,30]');
  });

  test('array decoder (empty array)', () => {
    const decode = makeBinaryDecoder({
      typeOID: 1007,
      dataType: 'int4[]',
      elemPgTypeClass: 'b',
    });

    // Empty array: ndim = 0
    const buf = Buffer.alloc(12); // ndim + flags + elem_oid
    buf.writeInt32BE(0, 0); // ndim = 0
    buf.writeInt32BE(0, 4); // flags
    buf.writeInt32BE(23, 8); // elem_oid

    expect(decode(buf)).toBe('[]');
  });

  test('array decoder with NULL element', () => {
    const decode = makeBinaryDecoder({
      typeOID: 1009,
      dataType: 'text[]',
      elemPgTypeClass: 'b',
    });

    // 1-dimensional text array: ['a', null, 'b']
    const buf = Buffer.alloc(
      4 +
        4 +
        4 + // ndim, flags, elem_oid
        4 +
        4 + // dim[0] size + lower_bound
        (4 + 1) +
        4 +
        (4 + 1), // 'a' (len+data), NULL (len=-1), 'b' (len+data)
    );
    let offset = 0;
    buf.writeInt32BE(1, offset);
    offset += 4; // ndim = 1
    buf.writeInt32BE(1, offset);
    offset += 4; // flags = has nulls
    buf.writeInt32BE(25, offset);
    offset += 4; // elem_oid = TEXT
    buf.writeInt32BE(3, offset);
    offset += 4; // dim size = 3
    buf.writeInt32BE(1, offset);
    offset += 4; // lower bound = 1

    buf.writeInt32BE(1, offset);
    offset += 4; // length = 1
    buf.write('a', offset);
    offset += 1; // data

    buf.writeInt32BE(-1, offset);
    offset += 4; // NULL

    buf.writeInt32BE(1, offset);
    offset += 4; // length = 1
    buf.write('b', offset);
    offset += 1; // data

    const result = decode(buf);
    expect(result).toBe('["a",null,"b"]');
  });
});
