import type {LogContext} from '@rocicorp/logger';
import type {Database, Statement} from '../../../../../zqlite/src/db.ts';
import {createLiteIndexStatement} from '../../../db/create.ts';
import {
  computeZqlSpecs,
  listIndexes,
  listTables,
} from '../../../db/lite-tables.ts';
import {mapPostgresToLiteIndex} from '../../../db/pg-to-lite.ts';
import type {IndexSpec} from '../../../db/specs.ts';
import {id} from '../../../types/sql.ts';

export const INDEX_METADATA_TABLE = '_zero.index_metadata';

export const CREATE_INDEX_METADATA_TABLE = /*sql*/ `
  CREATE TABLE "${INDEX_METADATA_TABLE}" (
    "tableName" TEXT NOT NULL,
    "name"      TEXT NOT NULL,
    "spec"      TEXT NOT NULL,
    PRIMARY KEY ("name")
  );
  CREATE INDEX IF NOT EXISTS "_zero.index_metadata_tableName" 
    ON "${INDEX_METADATA_TABLE}" ("tableName");
`;

/**
 * Stores upstream PostgreSQL index definitions for replica indexes so that
 * when primary keys change or replicas are migrated/rolled back, the canonical
 * upstream index specification is known without needing to query PostgreSQL.
 */
export class IndexMetadataStore {
  static #instances = new WeakMap<Database, IndexMetadataStore>();

  readonly #setStmt: Statement;
  readonly #getStmt: Statement;
  readonly #getTableStmt: Statement;
  readonly #deleteIndexStmt: Statement;
  readonly #deleteTableStmt: Statement;
  readonly #renameTableStmt: Statement;
  readonly #listStmt: Statement;

  private constructor(db: Database) {
    this.#setStmt = db.prepare(/*sql*/ `
      INSERT INTO "_zero.index_metadata" ("tableName", "name", "spec")
        VALUES (?, ?, ?)
      ON CONFLICT ("name") DO UPDATE SET
        "tableName" = excluded."tableName",
        "spec" = excluded."spec"
    `);

    this.#getStmt = db.prepare(/*sql*/ `
      SELECT "spec" FROM "_zero.index_metadata" WHERE "name" = ?
    `);

    this.#getTableStmt = db.prepare(/*sql*/ `
      SELECT "name", "spec" FROM "_zero.index_metadata" WHERE "tableName" = ?
    `);

    this.#deleteIndexStmt = db.prepare(/*sql*/ `
      DELETE FROM "_zero.index_metadata" WHERE "name" = ?
    `);

    this.#deleteTableStmt = db.prepare(/*sql*/ `
      DELETE FROM "_zero.index_metadata" WHERE "tableName" = ?
    `);

    this.#renameTableStmt = db.prepare(/*sql*/ `
      UPDATE "_zero.index_metadata" SET "tableName" = ? WHERE "tableName" = ?
    `);

    this.#listStmt = db.prepare(/*sql*/ `
      SELECT "tableName", "name", "spec" FROM "_zero.index_metadata"
    `);
  }

  static getInstance(db: Database): IndexMetadataStore | undefined {
    let instance = IndexMetadataStore.#instances.get(db);
    if (!instance) {
      const tableExists = db
        .prepare(
          `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_zero.index_metadata'`,
        )
        .get();

      if (!tableExists) {
        return undefined;
      }

      instance = new IndexMetadataStore(db);
      IndexMetadataStore.#instances.set(db, instance);
    }
    return instance;
  }

  static getOrCreateInstance(db: Database): IndexMetadataStore {
    const instance = IndexMetadataStore.getInstance(db);
    if (instance) {
      return instance;
    }
    db.exec(CREATE_INDEX_METADATA_TABLE);
    const newInstance = new IndexMetadataStore(db);
    IndexMetadataStore.#instances.set(db, newInstance);
    return newInstance;
  }

  setIndex(tableName: string, name: string, spec: IndexSpec): void {
    this.#setStmt.run(tableName, name, JSON.stringify(spec));
  }

  getIndex(name: string): IndexSpec | undefined {
    const row = this.#getStmt.get(name) as {spec: string} | undefined;
    return row ? (JSON.parse(row.spec) as IndexSpec) : undefined;
  }

  getIndexesForTable(tableName: string): {name: string; spec: IndexSpec}[] {
    const rows = this.#getTableStmt.all(tableName) as {
      name: string;
      spec: string;
    }[];
    return rows.map(r => ({
      name: r.name,
      spec: JSON.parse(r.spec) as IndexSpec,
    }));
  }

  deleteIndex(name: string): void {
    this.#deleteIndexStmt.run(name);
  }

  deleteTable(tableName: string): void {
    this.#deleteTableStmt.run(tableName);
  }

  renameTable(oldTableName: string, newTableName: string): void {
    this.#renameTableStmt.run(newTableName, oldTableName);
  }

  listIndexes(): {tableName: string; name: string; spec: IndexSpec}[] {
    const rows = this.#listStmt.all() as {
      tableName: string;
      name: string;
      spec: string;
    }[];
    return rows.map(r => ({
      tableName: r.tableName,
      name: r.name,
      spec: JSON.parse(r.spec) as IndexSpec,
    }));
  }
}

/**
 * Migration helper for schema version 18.
 *
 * 1. Prunes metadata for any indexes dropped from SQLite (e.g. during rollback).
 * 2. Seeds upstream IndexSpec definitions for any index not yet tracked.
 *    Pre-existing definitions in `_zero.index_metadata` are preserved as canonical.
 * 3. Rebuilds non-unique indexes in SQLite to append the table's primary key
 *    if they do not already include it.
 */
export function migrateIndexesToIncludePrimaryKey(
  lc: LogContext,
  db: Database,
): void {
  const store = IndexMetadataStore.getOrCreateInstance(db);
  const zqlSpecs = computeZqlSpecs(lc, db, {includeBackfillingColumns: true});
  const tablePKs = new Map<string, readonly string[]>();
  for (const table of listTables(db)) {
    const pk =
      table.primaryKey && table.primaryKey.length > 0
        ? table.primaryKey
        : (zqlSpecs.get(table.name)?.tableSpec.primaryKey ?? []);
    tablePKs.set(table.name, pk);
  }

  const existingIndexes = listIndexes(db).filter(
    idx => !idx.name.startsWith('sqlite_'),
  );
  const liveIndexNames = new Set(existingIndexes.map(i => i.name));

  // 1. Prune index metadata for indexes dropped from SQLite (e.g. during rollback)
  for (const recorded of store.listIndexes()) {
    if (!liveIndexNames.has(recorded.name)) {
      store.deleteIndex(recorded.name);
    }
  }

  // 2. Seed any missing index metadata from SQLite's index catalog.
  // For indexes already in `_zero.index_metadata`, do NOT overwrite (preserves clean upstream definition).
  for (const idx of existingIndexes) {
    if (!store.getIndex(idx.name)) {
      const dot = idx.tableName.indexOf('.');
      const schema = dot === -1 ? 'public' : idx.tableName.slice(0, dot);
      const tableName =
        dot === -1 ? idx.tableName : idx.tableName.slice(dot + 1);
      const upstreamSpec: IndexSpec = {
        schema,
        tableName,
        name: idx.name,
        columns: idx.columns,
        unique: idx.unique,
      };
      store.setIndex(idx.tableName, idx.name, upstreamSpec);
    }
  }

  // 3. Rebuild non-unique indexes with table primary keys appended
  for (const idx of existingIndexes) {
    if (idx.unique) {
      continue;
    }
    const pk = tablePKs.get(idx.tableName);
    if (!pk || pk.length === 0) {
      continue;
    }
    const upstreamSpec = store.getIndex(idx.name);
    if (!upstreamSpec) {
      continue;
    }
    const target = mapPostgresToLiteIndex(upstreamSpec, pk);
    const currentCols = Object.entries(idx.columns);
    const targetCols = Object.entries(target.columns);
    const matches =
      currentCols.length === targetCols.length &&
      currentCols.every(
        ([c, dir], i) => targetCols[i][0] === c && targetCols[i][1] === dir,
      );

    if (!matches) {
      lc.info?.(
        `Rebuilding index ${target.name} on ${target.tableName} to append primary key`,
      );
      db.exec(`DROP INDEX IF EXISTS ${id(target.name)}`);
      db.exec(createLiteIndexStatement(target));
    }
  }
}
