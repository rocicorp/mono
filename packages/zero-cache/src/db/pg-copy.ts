import {Transform} from 'node:stream';

/**
 * A stream Transform that parses a Postgres `COPY ... TO` text stream into
 * individual text values. The special {@link NULL_BYTE} string is used to
 * indicate a `null` value (as the `null` value itself indicates the end of
 * the stream and cannot be pushed as an element).
 *
 * Note that the transform assumes that the next step of the pipeline
 * understands the cardinality of values per row and does not push any
 * special value when reaching the end of a row.
 */
export class TextTransform extends Transform {
  #currVal: string = '';
  #escaped = false;

  constructor() {
    super({objectMode: true});
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (e?: Error) => void,
  ) {
    try {
      let l = 0;
      let r = 0;

      for (; r < chunk.length; r++) {
        const ch = chunk[r];
        if (this.#escaped) {
          const escapedChar = ESCAPED_CHARACTERS[ch];
          if (escapedChar === undefined) {
            throw new Error(
              `Unexpected escape character \\${String.fromCharCode(ch)}`,
            );
          }
          this.#currVal += escapedChar;
          l = r + 1;
          this.#escaped = false;
          continue;
        }
        switch (ch) {
          case 0x5c: // '\'
            // flush segment
            l < r && (this.#currVal += chunk.subarray(l, r).toString('utf8'));
            l = r + 1;
            this.#escaped = true;
            break;

          case 0x09: // '\t'
          case 0x0a: // '\n'
            // flush segment
            l < r && (this.#currVal += chunk.subarray(l, r).toString('utf8'));
            l = r + 1;

            // Value is done in both cases.
            this.push(this.#currVal);
            this.#currVal = '';
            break;
        }
      }
      // flush segment
      l < r && (this.#currVal += chunk.subarray(l, r).toString('utf8'));
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}

// The lone NULL byte signifies that the column value is `null`.
// (Postgres does not permit NULL bytes in TEXT values).
//
// Note that although NULL bytes can appear in JSON strings,
// those will always be represented within double-quotes,
// and thus never as a lone NULL byte.
export const NULL_BYTE = '\u0000';

// escaped characters used in https://www.postgresql.org/docs/current/sql-copy.html
const ESCAPED_CHARACTERS: Record<number, string | undefined> = {
  0x4e: NULL_BYTE, // \N signifies the NULL character.
  0x5c: '\\',
  0x62: '\b',
  0x66: '\f',
  0x6e: '\n',
  0x72: '\r',
  0x74: '\t',
  0x76: '\v',
} as const;
