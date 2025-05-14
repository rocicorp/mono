import {Transform} from 'node:stream';
import {
  assert,
  assertArray,
  assertNotUndefined,
} from '../../../shared/src/asserts.ts';
import type {PostgresDB} from '../types/pg.ts';
import {getTypeParsers, type TypeParser} from './pg-type-parser.ts';

/**
 * A stream Transform that parses a Postgres `COPY ... TO` text stream
 * into arrays of string / null values.
 */
export class TextTransform extends Transform {
  #currRow: (string | null)[] | undefined;
  #currCol: string | null | undefined;
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

      const append = (text: string) => {
        this.#currCol ??= '';
        this.#currCol += text;
      };

      const flushSegmentIf = (condition: boolean) => {
        if (condition) {
          append(text.substring(l, r));
        }
        l = r + 1;
      };

      for (; r < text.length; r++) {
        const ch = text[r];
        if (this.#escaped) {
          switch (ch) {
            case 'N':
              // \N is the NULL character.
              if (this.#currCol !== undefined) {
                throw new Error(
                  `\\N escape sequence found in the middle of a column "${this.#currCol}"`,
                );
              }
              this.#currCol = null;
              break;
            case '\\':
              append('\\');
              break;
            case 'b':
              append('\b');
              break;
            case 'f':
              append('\f');
              break;
            case 'n':
              append('\n');
              break;
            case 'r':
              append('\r');
              break;
            case 't':
              append('\t');
              break;
            case 'v':
              append('\v');
              break;
            default:
              // Not one of the escaped characters specified in https://www.postgresql.org/docs/current/sql-copy.html
              throw new Error(`Unexpected escape character \\${ch}`);
          }
          l = r + 1;
          this.#escaped = false;
          continue;
        }
        switch (ch) {
          case '\\':
            flushSegmentIf(l < r);
            this.#escaped = true;
            break;

          case '\t':
          case '\n':
            // Flush a possibly empty string unless #currCol was set to null via the \N escape.
            flushSegmentIf(this.#currCol !== null);
            assertNotUndefined(this.#currCol);

            // Column is done in both cases.
            (this.#currRow ??= []).push(this.#currCol);
            this.#currCol = undefined;

            if (ch === '\n') {
              // Row is also done on \n
              this.push(this.#currRow);
              this.#currRow = undefined;
            }
            break;
        }
      }
      flushSegmentIf(l < r);
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}

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

      const parsed = row.map((val, pos) =>
        val === null ? null : this.#columnParsers[pos](val),
      );
      this.push(parsed);
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}
