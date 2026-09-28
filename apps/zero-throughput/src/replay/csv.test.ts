import {mkdtempSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {gzipSync} from 'node:zlib';
import {describe, expect, test} from 'vitest';
import {CSVParser, readCSV} from './csv.ts';

function parseAll(chunks: readonly string[]): string[][] {
  const records: string[][] = [];
  const parser = new CSVParser(fields => records.push(fields));
  for (const chunk of chunks) {
    parser.push(chunk);
  }
  parser.end();
  return records;
}

describe('CSVParser', () => {
  test('parses plain, empty and quoted fields', () => {
    expect(parseAll(['a,b,,"c,d"\n1,,3,"x""y"\n'])).toEqual([
      ['a', 'b', '', 'c,d'],
      ['1', '', '3', 'x"y'],
    ]);
  });

  test('keeps newlines inside quotes and strips CRLF', () => {
    expect(parseAll(['a,"line1\nline2"\r\nb,c\r\n'])).toEqual([
      ['a', 'line1\nline2'],
      ['b', 'c'],
    ]);
  });

  test('handles a record with no trailing newline', () => {
    expect(parseAll(['a,b\nc,d'])).toEqual([
      ['a', 'b'],
      ['c', 'd'],
    ]);
  });

  test('is independent of chunk boundaries', () => {
    const text =
      'id,args\n1,"[""a"",{""k"":1}]"\n2,"q""\n""x"\n3,plain\n4,""\n';
    const whole = parseAll([text]);
    for (let size = 1; size <= 7; size++) {
      const chunks: string[] = [];
      for (let i = 0; i < text.length; i += size) {
        chunks.push(text.slice(i, i + size));
      }
      expect(parseAll(chunks)).toEqual(whole);
    }
    expect(whole).toEqual([
      ['id', 'args'],
      ['1', '["a",{"k":1}]'],
      ['2', 'q"\n"x'],
      ['3', 'plain'],
      ['4', ''],
    ]);
  });

  test('rejects input that ends inside quotes', () => {
    const parser = new CSVParser(() => undefined);
    parser.push('a,"unterminated');
    expect(() => parser.end()).toThrow('inside a quoted field');
  });
});

describe('readCSV', () => {
  test('reads a gzipped file keyed by its header', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'replay-csv-'));
    const path = join(dir, 'queries.csv.gz');
    writeFileSync(
      path,
      gzipSync(
        'clientGroupID,queryName,queryArgs\ncg1,homeView,"[""u1""]"\ncg2,nudges,\n',
      ),
    );
    const records = [];
    for await (const record of readCSV(path)) {
      records.push(record);
    }
    expect(records).toEqual([
      {clientGroupID: 'cg1', queryName: 'homeView', queryArgs: '["u1"]'},
      {clientGroupID: 'cg2', queryName: 'nudges', queryArgs: ''},
    ]);
  });
});
