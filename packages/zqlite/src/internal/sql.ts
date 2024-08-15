import type {SQLQuery, FormatConfig} from '@databases/sql';
import baseSql from '@databases/sql';
import {escapeSQLiteIdentifier} from '@databases/escape-identifier';

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

export const sql = baseSql.default;
