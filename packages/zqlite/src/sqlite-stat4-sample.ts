/**
 * Decoding of `sqlite_stat4.sample` values.
 *
 * A sample is a SQLite record: a header holding one serial type per column,
 * followed by the column data. We only ever look at the serial types, which
 * say what *kind* of value each column holds without revealing the value
 * itself. Two callers need this:
 *
 * - `SQLiteStatFanout`, which separates NULL from non-NULL samples so that
 *   join fanout is not skewed by a sparse foreign key.
 * - the `/plannerz` admin endpoint, which reports the shape of the histogram
 *   without returning row data.
 *
 * @see https://sqlite.org/fileformat2.html#record_format
 * @see https://sqlite.org/fileformat2.html#stat4tab
 */

/**
 * The kind of value a stat4 sample column holds, derived from its serial type.
 * `unknown` covers the internal serial types 10 and 11, which do not appear in
 * a well-formed record.
 */
export type SampleValueKind =
  | 'null'
  | 'integer'
  | 'real'
  | 'text'
  | 'blob'
  | 'unknown';

/**
 * Decodes the serial types in a stat4 sample's header.
 *
 * The sample's trailing rowid is included as a final column, matching the
 * record SQLite stores.
 *
 * @returns one kind per column, or an empty array if `sample` is empty or its
 *          header is malformed.
 */
export function decodeSampleKinds(sample: Buffer): SampleValueKind[] {
  const header = readVarint(sample, 0);
  if (header === undefined) {
    return [];
  }
  // `headerSize` counts the size varint itself, so the serial types run from
  // the end of that varint to `headerSize`, and cannot exceed the record.
  const [headerSize, serialTypesStart] = header;
  if (headerSize < serialTypesStart || headerSize > sample.length) {
    return [];
  }

  const kinds: SampleValueKind[] = [];
  for (let pos = serialTypesStart; pos < headerSize;) {
    const next = readVarint(sample, pos);
    if (next === undefined) {
      return [];
    }
    const [serialType, end] = next;
    kinds.push(kindOfSerialType(serialType));
    pos = end;
  }
  return kinds;
}

/**
 * Whether the first column of a sample is NULL.
 */
export function isSampleNull(sample: Buffer): boolean {
  const kinds = decodeSampleKinds(sample);
  return kinds.length === 0 || kinds[0] === 'null';
}

function kindOfSerialType(serialType: number): SampleValueKind {
  if (serialType === 0) {
    return 'null';
  }
  if (serialType <= 6 || serialType === 8 || serialType === 9) {
    return 'integer';
  }
  if (serialType === 7) {
    return 'real';
  }
  if (serialType <= 11) {
    return 'unknown'; // 10 and 11 are for internal use.
  }
  return serialType % 2 === 0 ? 'blob' : 'text';
}

/**
 * Reads the varint at `pos`.
 *
 * @returns the value and the offset just past it, or `undefined` if the varint
 *          runs off the end of the buffer. Values are read as `number`; a
 *          varint large enough to lose precision is not something a well-formed
 *          header or serial type contains.
 */
function readVarint(
  buf: Buffer,
  pos: number,
): [value: number, end: number] | undefined {
  let value = 0;
  // A varint is at most 9 bytes, the last of which contributes all 8 bits.
  for (let i = 0; i < 9; i++) {
    if (pos + i >= buf.length) {
      return undefined;
    }
    const byte = buf[pos + i];
    if (i === 8) {
      return [value * 256 + byte, pos + 9];
    }
    value = value * 128 + (byte & 0x7f);
    if ((byte & 0x80) === 0) {
      return [value, pos + i + 1];
    }
  }
  return undefined;
}
