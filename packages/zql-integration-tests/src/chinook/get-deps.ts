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

export async function getChinook(): Promise<string> {
  if (existsSync(PG_FILE_NAME)) {
    return readFile(PG_FILE_NAME, {encoding: 'utf-8'});
  }

  const response = await fetch(PG_URL);

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
  await writeFile(PG_FILE_NAME, content);
  return content;
}

export async function writeChinook(pg: PostgresDB, replica: Database) {
  const pgContent = await getChinook();
  await pg.unsafe(pgContent);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'chinook_test', shardNum: 0, publications: []},
    replica,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );
}
