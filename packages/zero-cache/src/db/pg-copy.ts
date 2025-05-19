import {Transform} from 'node:stream';
import {assert, assertArray} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../types/bigint-json.ts';
import type {PostgresDB} from '../types/pg.ts';
import {getTypeParsers, type TypeParser} from './pg-type-parser.ts';

/**
 * A stream Transform that parses a Postgres `COPY ... TO` text stream
 * into arrays of string / null values.
 *
 * The outputs (i.e. arrays of `string|null`s) are not referenced by the
 * transform, so the next step in the pipeline is free to modify it directly.
 */
export class TextTransform extends Transform {
  #currRow: (string | null)[] = [];
  #currCol: string = '';
  #escaped = false;

  constructor() {
    super({objectMode: true});
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  _transform(
    chunk: Buffer,
    encoding: BufferEncoding,
    callback: (e?: Error) => void,
  ) {
    try {
      const text = chunk.toString(encoding);

      let l = 0;
      let r = 0;

      for (; r < text.length; r++) {
        const ch = text.charCodeAt(r);
        if (this.#escaped) {
          const escapedChar = ESCAPED_CHARACTERS[ch];
          if (escapedChar === undefined) {
            throw new Error(
              `Unexpected escape character \\${String.fromCharCode(ch)}`,
            );
          }
          this.#currCol += escapedChar;
          l = r + 1;
          this.#escaped = false;
          continue;
        }
        switch (ch) {
          case 0x5c: // '\'
            // flush segment
            l < r && (this.#currCol += text.substring(l, r));
            l = r + 1;
            this.#escaped = true;
            break;

          case 0x09: // '\t'
          case 0x0a: // '\n'
            // flush segment
            l < r && (this.#currCol += text.substring(l, r));
            l = r + 1;

            // Column is done in both cases.
            this.#currRow.push(
              // The lone NULL byte signifies that the column value is `null`.
              // (Postgres does not permit NULL bytes in TEXT values).
              //
              // Note that although NULL bytes can appear in JSON strings,
              // those will always be represented within double-quotes,
              // and thus never as a lone NULL byte.
              this.#currCol === NULL_BYTE ? null : this.#currCol,
            );
            this.#currCol = '';

            if (ch === 0x0a /* \n */) {
              // Row is also done on \n
              this.push(this.#currRow);
              this.#currRow = [];
            }
            break;
        }
      }
      // flush segment
      l < r && (this.#currCol += text.substring(l, r));
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}

const NULL_BYTE = '\u0000';

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

export type RowTransformOutput = {
  values: JSONValue[];
  size: number;
};

/**
 * A stream Transform that transforms the array of strings outputted by
 * the {@link TextTransform} and parses them to their Javascript values
 * as directed by the supplied list of column types.
 */
export class RowTransform extends Transform {
  static async create(
    db: PostgresDB,
    columns: {typeOID: number; dataType?: string}[],
  ) {
    const typeParsers = await getTypeParsers(db);
    const columnParsers = columns.map(c =>
      typeParsers.getTypeParser(c.typeOID),
    );
    return new RowTransform(columnParsers);
  }

  readonly #columnParsers: TypeParser[];

  private constructor(columnParsers: TypeParser[]) {
    super({objectMode: true});
    this.#columnParsers = columnParsers;
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  _transform(row: string[], _encoding: string, callback: (e?: Error) => void) {
    try {
      // sanity check that this is preceded by a TextTransform in the pipeline
      assertArray(row);
      assert(
        row.length === this.#columnParsers.length,
        `Expected row to have ${this.#columnParsers.length} values but found ${row.length}`,
      );

      // Optimization: Reuse the array for the output to reduce array allocations.
      const values = row as JSONValue[];
      let size = 0;
      for (let i = 0; i < row.length; i++) {
        const val = row[i];
        // Give every column a min size of 4 bytes, even if null or empty.
        size += 4 + (val?.length ?? 0);
        values[i] =
          val === null ? null : (this.#columnParsers[i](val) as JSONValue);
      }

      this.push({values, size} satisfies RowTransformOutput);
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}
