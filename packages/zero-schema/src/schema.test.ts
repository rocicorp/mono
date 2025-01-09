import {expect, test} from 'vitest';
import {createSchema} from './schema.js';

test('Unexpected tableName should throw', () => {
  const schema = {
    version: 1,
    tables: {
      foo: {
        tableName: 'foo',
        primaryKey: 'id',
        columns: {
          id: {type: 'number'},
        },
      },
      bar: {
        tableName: 'bars',
        primaryKey: 'id',
        columns: {
          id: {type: 'number'},
        },
      },
    },
  } as const;
  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Table name mismatch: "bars" !== "bar"]`,
  );
});

test('Missing table in direct relationship should throw', () => {
  const bar = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const foo = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      barID: {type: 'number'},
    },
    relationships: {
      barRelation: {
        sourceField: 'barID',
        destSchema: () => bar,
        destField: 'id',
      },
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      foo,
    },
  } as const;

  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Relationship "foo"."barRelation" destination "bar" is missing in schema]`,
  );
});

test('Missing table in junction relationship should throw', () => {
  const tableA = {
    tableName: 'tableA',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const tableB = {
    tableName: 'tableB',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      aID: {type: 'number'},
    },
    relationships: {
      relationBToA: {
        sourceField: 'aID',
        destSchema: () => tableA,
        destField: 'id',
      },
    },
  } as const;

  const tableC = {
    tableName: 'tableC',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      bID: {type: 'number'},
    },
    relationships: {
      relationCToB: [
        {
          sourceField: 'bID',
          destSchema: () => tableB,
          destField: 'id',
        },
        {
          sourceField: 'aID',
          destSchema: () => tableA,
          destField: 'id',
        },
      ],
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      tableB,
      tableC,
    },
  } as const;

  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Relationship "tableB"."relationBToA" destination "tableA" is missing in schema]`,
  );
});

test('Missing column in direct relationship destination should throw', () => {
  const bar = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const foo = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      barID: {type: 'number'},
    },
    relationships: {
      barRelation: {
        sourceField: 'barID',
        destSchema: () => bar,
        destField: 'missing',
      },
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      foo,
      bar,
    },
  } as const;

  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Column "missing" is missing in table "bar"]`,
  );
});

test('Missing column in direct relationship source should throw', () => {
  const bar = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const foo = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      barID: {type: 'number'},
    },
    relationships: {
      barRelation: {
        sourceField: 'missing',
        destSchema: () => bar,
        destField: 'id',
      },
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      foo,
      bar,
    },
  } as const;

  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Column "missing" is missing in table "foo"]`,
  );
});

test('Missing column in junction relationship destination should throw', () => {
  const tableA = {
    tableName: 'tableA',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
    relationships: {
      relationAToB: [
        {
          sourceField: 'id',
          destSchema: () => junctionTable,
          destField: 'aID',
        },
        {
          sourceField: 'aID',
          destSchema: () => tableB,
          destField: 'missing',
        },
      ],
    },
  } as const;

  const tableB = {
    tableName: 'tableB',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const junctionTable = {
    tableName: 'junctionTable',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      aID: {type: 'number'},
      bID: {type: 'number'},
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      tableA,
      tableB,
      junctionTable,
    },
  } as const;

  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Column "missing" is missing in table "tableB"]`,
  );
});

test('Missing column in junction relationship source should throw', () => {
  const tableA = {
    tableName: 'tableA',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
    relationships: {
      relationAToB: [
        {
          sourceField: 'id',
          destSchema: () => junctionTable,
          destField: 'aID',
        },
        {
          sourceField: 'missing',
          destSchema: () => tableB,
          destField: 'id',
        },
      ],
    },
  } as const;

  const tableB = {
    tableName: 'tableB',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const junctionTable = {
    tableName: 'junctionTable',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      aID: {type: 'number'},
      bID: {type: 'number'},
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      tableA,
      tableB,
      junctionTable,
    },
  } as const;

  expect(() => createSchema(schema)).toThrowErrorMatchingInlineSnapshot(
    `[Error: Column "missing" is missing in table "junctionTable"]`,
  );
});
