import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {existsSync} from 'fs';
import {readFile, writeFile} from 'fs/promises';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {Database} from '../../../zqlite/src/db.ts';

const PG_URL =
  'https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_PostgreSql.sql';
const PG_FILE_NAME = 'Chinook_PostgreSql.sql';

async function getChinook(fileName: string, url: string): Promise<string> {
  if (existsSync(fileName)) {
    return readFile(fileName, {encoding: 'utf-8'});
  }

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Failed to download: ${response.status} ${response.statusText}`,
    );
  }

  const content = (await response.text())
    .replaceAll('DROP DATABASE IF EXISTS chinook;', '')
    .replaceAll('CREATE DATABASE chinook;', '')
    .replaceAll('\\c chinook;', '')
    // disabled foreign key constraints as push tests do not respect an insertion order that would preserved them.
    .replace(/ALTER TABLE.*?FOREIGN KEY.*?;/gs, '');
  await writeFile(fileName, content);
  return content;
}

export async function writeChinook(pg: PostgresDB, replica: Database) {
  const pgContent = await getChinook(PG_FILE_NAME, PG_URL);
  await pg.unsafe(pgContent);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'chinook_test', shardNum: 0, publications: []},
    replica,
    getConnectionURI(pg),
    {tableCopyWorkers: 1, rowBatchSize: 10000},
  );
}
