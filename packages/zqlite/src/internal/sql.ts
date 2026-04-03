import {escapeSQLiteIdentifier} from '@databases/escape-identifier';
import type {FormatConfig, SQLQuery} from '@databases/sql';
import sql from '@databases/sql';

const sqliteFormat: FormatConfig = {
  escapeIdentifier: str => escapeSQLiteIdentifier(str),
  formatValue: value => ({placeholder: '?', value}),
};

export function compile(sql: SQLQuery): string {
  return sql.format(sqliteFormat).text;
}

export function format(sql: SQLQuery) {
  return sql.format(sqliteFormat);
}

export {sql};
