import {createReadStream} from 'node:fs';
import {createGunzip} from 'node:zlib';

/**
 * Parses RFC 4180 CSV incrementally: quoted fields may contain commas,
 * newlines and doubled quotes. Feed it text chunks of any size; complete
 * records are handed to `onRecord` as they finish.
 */
export class CSVParser {
  readonly #onRecord: (fields: string[]) => void;
  #field = '';
  #fields: string[] = [];
  #inQuotes = false;
  // A quote seen inside a quoted field; it is either the closing quote or the
  // first half of an escaped `""`, which the next character decides.
  #pendingQuote = false;

  constructor(onRecord: (fields: string[]) => void) {
    this.#onRecord = onRecord;
  }

  push(chunk: string): void {
    let start = 0;
    for (let i = 0; i < chunk.length; i++) {
      const c = chunk.charCodeAt(i);
      if (this.#pendingQuote) {
        this.#pendingQuote = false;
        if (c === QUOTE) {
          this.#field += '"';
          start = i + 1;
          continue;
        }
        this.#inQuotes = false;
      }
      if (this.#inQuotes) {
        if (c === QUOTE) {
          this.#field += chunk.slice(start, i);
          this.#pendingQuote = true;
          start = i + 1;
        }
        continue;
      }
      if (c === QUOTE) {
        this.#field += chunk.slice(start, i);
        this.#inQuotes = true;
        start = i + 1;
      } else if (c === COMMA) {
        this.#field += chunk.slice(start, i);
        this.#fields.push(this.#field);
        this.#field = '';
        start = i + 1;
      } else if (c === NEWLINE) {
        let end = i;
        if (end > start && chunk.charCodeAt(end - 1) === CARRIAGE_RETURN) {
          end--;
        }
        this.#field += chunk.slice(start, end);
        this.#endRecord();
        start = i + 1;
      }
    }
    if (start < chunk.length) {
      this.#field += chunk.slice(start);
    }
  }

  end(): void {
    if (this.#pendingQuote) {
      this.#pendingQuote = false;
      this.#inQuotes = false;
    }
    if (this.#inQuotes) {
      throw new Error('CSV input ended inside a quoted field');
    }
    if (this.#field !== '' || this.#fields.length > 0) {
      this.#endRecord();
    }
  }

  #endRecord(): void {
    this.#fields.push(this.#field);
    this.#field = '';
    const fields = this.#fields;
    this.#fields = [];
    this.#onRecord(fields);
  }
}

const QUOTE = 0x22;
const COMMA = 0x2c;
const NEWLINE = 0x0a;
const CARRIAGE_RETURN = 0x0d;

export type CSVRecord = Readonly<Record<string, string>>;

/**
 * Streams the records of a CSV file with a header line, gunzipping it when
 * the name ends in `.gz`. Fields are keyed by the header's column names.
 */
export async function* readCSV(path: string): AsyncGenerator<CSVRecord> {
  let header: string[] | undefined;
  let batch: CSVRecord[] = [];
  const parser = new CSVParser(fields => {
    if (header === undefined) {
      header = fields;
      return;
    }
    const record: Record<string, string> = {};
    for (let i = 0; i < header.length; i++) {
      record[header[i]] = fields[i] ?? '';
    }
    batch.push(record);
  });
  const file = createReadStream(path);
  const input = path.endsWith('.gz') ? file.pipe(createGunzip()) : file;
  input.setEncoding('utf8');
  for await (const chunk of input) {
    parser.push(chunk as string);
    const records = batch;
    batch = [];
    yield* records;
  }
  parser.end();
  yield* batch;
}
