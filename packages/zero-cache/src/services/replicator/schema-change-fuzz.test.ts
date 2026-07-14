import fc from 'fast-check';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
} from '../../db/create.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../db/pg-to-lite.ts';
import type {ColumnSpec, TableSpec} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {
  canonicalReplicaState,
  type CanonicalReplicaState,
} from '../../test/replica-state.ts';
import type {
  DataOrSchemaChange,
  MessageRelation,
} from '../change-source/protocol/current/data.ts';
import {ChangeProcessor} from './change-processor.ts';
import {ColumnMetadataStore} from './schema/column-metadata.ts';
import {initReplicationState} from './schema/replication-state.ts';

type ModelColumn = {name: string; spec: ColumnSpec};
type ModelIndex = {
  schema: string;
  tableName: string;
  name: string;
  unique: boolean;
  columns: Record<string, 'ASC' | 'DESC'>;
};
type ModelRow = Record<string, number | string>;
type SchemaModel = {
  tableName: string;
  columns: ModelColumn[];
  indexes: ModelIndex[];
  rows: ModelRow[];
};

type FuzzOperation = {
  tag:
    | 'add-column'
    | 'change-type'
    | 'create-index'
    | 'drop-column'
    | 'drop-index'
    | 'rename-column'
    | 'rename-table'
    | 'set-character-limit'
    | 'toggle-nullability';
  target: number;
  secondary: number;
  width: number;
};

const fuzzOperation = fc.record({
  tag: fc.constantFrom<FuzzOperation['tag']>(
    'add-column',
    'change-type',
    'create-index',
    'drop-column',
    'drop-index',
    'rename-column',
    'rename-table',
    'set-character-limit',
    'toggle-nullability',
  ),
  target: fc.nat({max: 1_000}),
  secondary: fc.nat({max: 1_000}),
  width: fc.integer({min: 1, max: 4}),
});

test('streamed schema changes match a replica rebuilt from the final schema', () => {
  fc.assert(
    fc.property(
      fc.array(fuzzOperation, {minLength: 1, maxLength: 30}),
      operations => {
        const model = initialModel();
        const streamed = buildReplica(model);
        const processor = new ChangeProcessor(
          new StatementRunner(streamed),
          'serving',
          (_, error) => {
            throw error;
          },
        );

        try {
          operations.forEach((operation, step) => {
            const changes = applyOperation(model, operation, step);
            if (changes.length === 0) {
              return;
            }
            applyTransaction(processor, changes, watermark(step));
          });

          const rebuilt = buildReplica(model);
          try {
            expectReplicaStatesToMatch(
              canonicalReplicaState(streamed),
              canonicalReplicaState(rebuilt),
            );
          } finally {
            rebuilt.close();
          }
        } finally {
          streamed.close();
        }
      },
    ),
    {numRuns: 100},
  );
});

function expectReplicaStatesToMatch(
  streamed: CanonicalReplicaState,
  rebuilt: CanonicalReplicaState,
) {
  expect(streamed.integrityCheck).toEqual([{integrity_check: 'ok'}]);
  expect(streamed.physicalTables).toEqual(rebuilt.physicalTables);
  expect(streamed.logicalTables).toEqual(rebuilt.logicalTables);
  expect(streamed.indexes).toEqual(rebuilt.indexes);
  expect(streamed.columnMetadata).toEqual(rebuilt.columnMetadata);
  expect(streamed.rows).toEqual(rebuilt.rows);
}

function initialModel(): SchemaModel {
  const tableName = 'foo';
  return {
    tableName,
    columns: [
      {name: 'id', spec: columnSpec(1, 'int8', true)},
      {name: 'tenant_id', spec: columnSpec(2, 'text', false)},
      {name: 'group_id', spec: columnSpec(3, 'text', true)},
      {
        name: 'note',
        spec: columnSpec(4, 'varchar', false, 32),
      },
    ],
    indexes: [
      indexSpec(tableName, 'foo_pkey', ['id'], true),
      indexSpec(tableName, 'foo_tenant_group', ['tenant_id', 'group_id', 'id']),
      indexSpec(tableName, 'foo_group_tenant', ['group_id', 'tenant_id']),
    ],
    rows: [
      {id: 1, tenant_id: 'tenant-1', group_id: 'group-1', note: 'note-1'},
      {id: 2, tenant_id: 'tenant-2', group_id: 'group-2', note: 'note-2'},
    ],
  };
}

function columnSpec(
  pos: number,
  dataType: string,
  notNull: boolean,
  characterMaximumLength: number | null = null,
): ColumnSpec {
  return {
    pos,
    dataType,
    notNull,
    characterMaximumLength,
    dflt: null,
  };
}

function indexSpec(
  tableName: string,
  name: string,
  columns: string[],
  unique = false,
): ModelIndex {
  return {
    schema: 'public',
    tableName,
    name,
    unique,
    columns: Object.fromEntries(columns.map(column => [column, 'ASC'])),
  };
}

function tableSpec(model: SchemaModel): TableSpec {
  return {
    schema: 'public',
    name: model.tableName,
    columns: Object.fromEntries(
      model.columns.map(({name, spec}) => [name, spec]),
    ),
    primaryKey: ['id'],
  };
}

function buildReplica(model: SchemaModel): Database {
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  initReplicationState(db, ['zero_data'], '0000');

  const pgTable = tableSpec(model);
  db.exec(createLiteTableStatement(mapPostgresToLite(pgTable, '0000')));

  const metadata = must(ColumnMetadataStore.getInstance(db));
  for (const {name, spec} of model.columns) {
    metadata.insert(model.tableName, name, spec);
  }
  for (const index of model.indexes) {
    db.exec(createLiteIndexStatement(mapPostgresToLiteIndex(index)));
  }

  const columns = model.columns.map(({name}) => name);
  const placeholders = columns.map(() => '?').join(', ');
  const insert = db.prepare(
    `INSERT INTO "${model.tableName}" (${columns
      .map(column => `"${column}"`)
      .join(', ')}) VALUES (${placeholders})`,
  );
  for (const row of model.rows) {
    insert.run(...columns.map(column => row[column]));
  }
  return db;
}

function applyTransaction(
  processor: ChangeProcessor,
  changes: DataOrSchemaChange[],
  version: string,
) {
  const lc = createSilentLogContext();
  processor.processMessage(lc, [
    'begin',
    {tag: 'begin'},
    {commitWatermark: version},
  ]);
  for (const change of changes) {
    processor.processMessage(lc, ['data', change]);
  }
  processor.processMessage(lc, [
    'commit',
    {tag: 'commit'},
    {watermark: version},
  ]);
}

function applyOperation(
  model: SchemaModel,
  operation: FuzzOperation,
  step: number,
): DataOrSchemaChange[] {
  const changes: DataOrSchemaChange[] = [];
  const mutableColumns = model.columns.filter(({name}) => name !== 'id');
  const column = mutableColumns[operation.target % mutableColumns.length];

  switch (operation.tag) {
    case 'change-type': {
      const old = cloneColumn(column);
      const dataType = column.spec.dataType === 'text' ? 'varchar' : 'text';
      column.spec = {
        ...column.spec,
        dataType,
        characterMaximumLength: dataType === 'varchar' ? 64 : null,
      };
      changes.push(updateColumn(model, old, cloneColumn(column)));
      break;
    }
    case 'toggle-nullability': {
      const old = cloneColumn(column);
      column.spec = {...column.spec, notNull: !column.spec.notNull};
      changes.push(updateColumn(model, old, cloneColumn(column)));
      break;
    }
    case 'set-character-limit': {
      const varcharColumns = mutableColumns.filter(
        ({spec}) => spec.dataType === 'varchar',
      );
      if (varcharColumns.length === 0) {
        break;
      }
      const varchar = varcharColumns[operation.target % varcharColumns.length];
      const old = cloneColumn(varchar);
      varchar.spec = {
        ...varchar.spec,
        characterMaximumLength: operation.secondary % 128 || 1,
      };
      changes.push(updateColumn(model, old, cloneColumn(varchar)));
      break;
    }
    case 'rename-column': {
      const old = cloneColumn(column);
      const newName = `renamed_${step}`;
      column.name = newName;
      for (const index of model.indexes) {
        index.columns = renameKey(index.columns, old.name, newName);
      }
      for (const row of model.rows) {
        row[newName] = must(row[old.name]);
        delete row[old.name];
      }
      changes.push(updateColumn(model, old, cloneColumn(column)));
      break;
    }
    case 'add-column': {
      const name = `added_${step}`;
      const spec = columnSpec(
        Math.max(...model.columns.map(({spec}) => spec.pos)) + 1,
        operation.target % 2 === 0 ? 'text' : 'varchar',
        false,
        operation.target % 2 === 0 ? null : 64,
      );
      model.columns.push({name, spec});
      changes.push({
        tag: 'add-column',
        table: tableID(model),
        column: {name, spec},
        tableMetadata: tableMetadata(),
      });
      for (const row of model.rows) {
        row[name] = `${name}-${row['id']}`;
      }
      break;
    }
    case 'drop-column': {
      if (mutableColumns.length <= 1) {
        break;
      }
      const referencing = model.indexes.filter(
        index => column.name in index.columns,
      );
      for (const index of referencing) {
        changes.push({
          tag: 'drop-index',
          id: {schema: index.schema, name: index.name},
        });
      }
      model.indexes = model.indexes.filter(
        index => !referencing.includes(index),
      );
      model.columns = model.columns.filter(item => item !== column);
      for (const row of model.rows) {
        delete row[column.name];
      }
      changes.push({
        tag: 'drop-column',
        table: tableID(model),
        column: column.name,
      });
      break;
    }
    case 'create-index': {
      const ordered = rotate(
        model.columns.map(({name}) => name),
        operation.target,
      );
      const columns = ordered.slice(
        0,
        Math.min(operation.width, ordered.length),
      );
      const index = indexSpec(model.tableName, `fuzz_index_${step}`, columns);
      if (operation.secondary % 2 === 1) {
        index.columns = Object.fromEntries(
          Object.keys(index.columns).map(name => [name, 'DESC']),
        );
      }
      model.indexes.push(index);
      changes.push({tag: 'create-index', spec: index});
      break;
    }
    case 'drop-index': {
      const droppable = model.indexes.filter(({name}) => name !== 'foo_pkey');
      if (droppable.length === 0) {
        break;
      }
      const index = droppable[operation.target % droppable.length];
      model.indexes = model.indexes.filter(item => item !== index);
      changes.push({
        tag: 'drop-index',
        id: {schema: index.schema, name: index.name},
      });
      break;
    }
    case 'rename-table': {
      const old = tableID(model);
      model.tableName = `renamed_table_${step}`;
      for (const index of model.indexes) {
        index.tableName = model.tableName;
      }
      changes.push({tag: 'rename-table', old, new: tableID(model)});
      break;
    }
  }

  if (changes.length === 0) {
    return changes;
  }

  const relation = relationFor(model);
  for (const row of model.rows) {
    changes.push({tag: 'update', relation, new: row, key: null});
  }
  const newRow = Object.fromEntries(
    model.columns.map(({name}) => [
      name,
      name === 'id'
        ? model.rows.length + 1
        : `${name}-${model.rows.length + 1}`,
    ]),
  );
  model.rows.push(newRow);
  changes.push({tag: 'insert', relation, new: newRow});
  return changes;
}

function cloneColumn(column: ModelColumn): ModelColumn {
  return {name: column.name, spec: {...column.spec}};
}

function updateColumn(
  model: SchemaModel,
  old: ModelColumn,
  updated: ModelColumn,
): DataOrSchemaChange {
  return {
    tag: 'update-column',
    table: tableID(model),
    old,
    new: updated,
  };
}

function tableID(model: SchemaModel) {
  return {schema: 'public', name: model.tableName};
}

function tableMetadata() {
  return {rowKey: {columns: ['id'], type: 'default' as const}};
}

function relationFor(model: SchemaModel): MessageRelation {
  return {
    schema: 'public',
    name: model.tableName,
    rowKey: {type: 'default', columns: ['id']},
  };
}

function renameKey(
  record: Record<string, 'ASC' | 'DESC'>,
  oldName: string,
  newName: string,
) {
  return Object.fromEntries(
    Object.entries(record).map(([name, direction]) => [
      name === oldName ? newName : name,
      direction,
    ]),
  );
}

function rotate<T>(values: T[], offset: number): T[] {
  const start = offset % values.length;
  return [...values.slice(start), ...values.slice(0, start)];
}

function watermark(step: number) {
  return String(step + 1).padStart(4, '0');
}
