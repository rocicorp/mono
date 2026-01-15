import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {existsSync} from 'fs';
import {readFile, writeFile} from 'fs/promises';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {Database} from '../../../zqlite/src/db.ts';

const SCHEMA_URL =
  'https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql';
const DATA_URL =
  'https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-insert-data.sql';
const CACHE_FILE = 'Pagila_PostgreSql.sql';

async function fetchUrl(url: string): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(
      `Failed to download ${url}: ${response.status} ${response.statusText}`,
    );
  }
  return response.text();
}

export async function getPagila(): Promise<string> {
  if (existsSync(CACHE_FILE)) {
    return readFile(CACHE_FILE, {encoding: 'utf-8'});
  }

  // Download schema and data
  const [schemaContent, dataContent] = await Promise.all([
    fetchUrl(SCHEMA_URL),
    fetchUrl(DATA_URL),
  ]);

  // Process schema - remove PostgreSQL-specific features that won't work in our test env
  let processedSchema = schemaContent
    // Remove database commands
    .replace(/DROP DATABASE.*?;/gs, '')
    .replace(/CREATE DATABASE.*?;/gs, '')
    .replace(/\\c\s+\w+;?/g, '')
    // Remove pg_catalog function calls (like set_config)
    .replace(/SELECT pg_catalog\.[^;]+;/g, '')
    // Remove extensions that may not be available
    .replace(/CREATE EXTENSION.*?;/gs, '')
    // Remove foreign key constraint statements (must be on same line or close)
    // Match: ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ...;
    .replace(/ALTER TABLE[^;]*ADD CONSTRAINT[^;]*FOREIGN KEY[^;]*;/g, '')
    // Remove all OWNER TO statements (single line)
    .replace(/ALTER\s+\w+[^;]*OWNER TO[^;]*;/g, '')
    // Remove triggers (they depend on functions)
    .replace(/CREATE TRIGGER[^;]*;/g, '')
    // Remove function definitions (both $$ and $_$ delimited)
    .replace(/CREATE OR REPLACE FUNCTION[\s\S]*?\$\$[\s\S]*?\$\$[\s\S]*?;/g, '')
    .replace(/CREATE FUNCTION[\s\S]*?\$\$[\s\S]*?\$\$[\s\S]*?;/g, '')
    .replace(
      /CREATE OR REPLACE FUNCTION[\s\S]*?\$_\$[\s\S]*?\$_\$[\s\S]*?;/g,
      '',
    )
    .replace(/CREATE FUNCTION[\s\S]*?\$_\$[\s\S]*?\$_\$[\s\S]*?;/g, '')
    // Remove aggregate definitions
    .replace(/CREATE AGGREGATE[\s\S]*?\);/g, '')
    // Remove rules
    .replace(/CREATE RULE[^;]*;/g, '')
    // Remove views (they may depend on functions)
    .replace(/CREATE VIEW[\s\S]*?;/g, '');

  // Process data:
  // Remove any SET/SELECT config commands that might cause issues
  const processedData = dataContent
    .replace(/SET\s+search_path.*?;/g, '')
    // Remove all pg_catalog function calls (set_config, setval, etc.)
    .replace(/SELECT pg_catalog\.[^;]+;/g, '');

  // Combine schema and data
  const content = processedSchema + '\n\n' + processedData;

  await writeFile(CACHE_FILE, content);
  return content;
}

export async function writePagila(pg: PostgresDB, replica: Database) {
  const pgContent = await getPagila();
  await pg.unsafe(pgContent);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'pagila_test', shardNum: 0, publications: []},
    replica,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );
}
