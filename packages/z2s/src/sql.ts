import type {SQLQuery, FormatConfig} from '@databases/sql';
import baseSql from '@databases/sql';
import {
  escapePostgresIdentifier,
  escapeSQLiteIdentifier,
} from '@databases/escape-identifier';

const pgFormat: FormatConfig = {
  escapeIdentifier: str => escapePostgresIdentifier(str),
  formatValue: (value, index) => ({placeholder: `$${index + 1}`, value}),
};

const sqliteFormat: FormatConfig = {
  escapeIdentifier: str => escapeSQLiteIdentifier(str),
  formatValue: value => ({placeholder: '?', value}),
};

export function formatPg(sql: SQLQuery) {
  return sql.format(pgFormat);
}

export function formatSqlite(sql: SQLQuery) {
  return sql.format(sqliteFormat);
}

export const sql = baseSql.default;
