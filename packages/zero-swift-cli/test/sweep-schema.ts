/**
 * Schemas with names `zero-swift codegen` has to work around: Swift keywords
 * and reserved members, names that collide once they are made identifiers,
 * names that shadow the types the generated code uses, enumeration values
 * that have no letters, and a recursive JSON type. Each export is one case
 * of `sweep-codegen.test.ts`; every one should generate Swift that compiles.
 */
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  enumeration,
  json,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';

function one(name: string) {
  return table(name).columns({id: string()}).primaryKey('id');
}

/** `zero` is PascalCased to `Zero`, a reserved name, so it gets `Row`. */
export const zeroTable = createSchema({tables: [one('zero')]});

/** Two tables PascalCase to `FooBar`; the second gets the first's name. */
export const clashingTables = createSchema({
  tables: [one('foo_bar_row'), one('foo_bar'), one('fooBar')],
});

/** Row types that shadow the protocols the generated code conforms to. */
export const protocolTables = createSchema({
  tables: [one('sendable'), one('hashable'), one('codable')],
});

/** `self` is PascalCased to `Self`. */
export const selfTable = createSchema({tables: [one('self')]});

/** Columns with no letters or digits an identifier can use. */
export const symbolColumns = createSchema({
  tables: [
    table('t')
      .columns({id: string(), _: string(), é: string(), ü: string()})
      .primaryKey('id'),
  ],
});

/** Columns that are different names but the same identifier. */
export const clashingColumns = createSchema({
  tables: [
    table('t')
      .columns({'id': string(), 'a-b': string(), 'a_b': string()})
      .primaryKey('id'),
  ],
});

/** Columns named after members a row struct already has. */
export const memberColumns = createSchema({
  tables: [
    table('t')
      .columns({id: string(), hashValue: string(), columnValues: string()})
      .primaryKey('id'),
  ],
});

/** Columns Swift doesn't allow as property names, backticked or not. */
export const keywordColumns = createSchema({
  tables: [
    table('t')
      .columns({
        id: string(),
        Type: string(),
        Protocol: string(),
        self: string(),
      })
      .primaryKey('id'),
  ],
});

/** Enumeration values without letters, and with characters outside ASCII. */
export const symbolEnum = createSchema({
  tables: [
    table('t')
      .columns({
        id: string(),
        op: enumeration<'+' | '-'>(),
        lang: enumeration<'日本' | 'é'>(),
      })
      .primaryKey('id'),
  ],
});

/** An enumeration value with a backspace, which JSON escapes as `\b`. */
export const controlCharacterEnum = createSchema({
  tables: [
    table('t')
      .columns({id: string(), key: enumeration<'x\bz' | 'y\fz'>()})
      .primaryKey('id'),
  ],
});

type Fields = {'zeroValue': number; 'a-b': number; 'a_b': number};

/** A JSON object type whose fields clash with each other and with
 * `zeroValue`. */
export const clashingFields = createSchema({
  tables: [
    table('t').columns({id: string(), o: json<Fields>()}).primaryKey('id'),
  ],
});

type Tree = {name: string; children: Tree[]};

/** A recursive JSON type. */
export const recursiveJSON = createSchema({
  tables: [
    table('t').columns({id: string(), tree: json<Tree>()}).primaryKey('id'),
  ],
});

type Local = {o: number; zeroValue: number};

/** Names of the parameters and locals the generated code assigns from. */
export const localNames = createSchema({
  tables: [
    table('t')
      .columns({id: string(), entry: string(), o: json<Local>()})
      .primaryKey('id'),
  ],
});

const child = table('child')
  .columns({
    id: string(),
    parentID: string(),
    _parent: string(),
    parent_id: string(),
  })
  .primaryKey('id');
const parent = one('parent');

/** A column named as the storage `@Indirect` declares for a relationship,
 * and a relationship that is a column's identifier. */
export const wrapperStorage = createSchema({
  tables: [child, parent],
  relationships: [
    relationships(child, ({one}) => ({
      'parent': one({
        sourceField: ['parentID'],
        destField: ['id'],
        destSchema: parent,
      }),
      'parent-id': one({
        sourceField: ['parentID'],
        destField: ['id'],
        destSchema: parent,
      }),
    })),
  ],
});

/** Enumeration values that are members of every enumeration, or keywords. */
export const memberEnum = createSchema({
  tables: [
    table('t')
      .columns({
        id: string(),
        e: enumeration<'self' | 'Type' | 'rawValue' | 'allCases' | 'default'>(),
      })
      .primaryKey('id'),
  ],
});

/** Table names that are one identifier in the query builder. */
export const clashingQueries = createSchema({
  tables: [one('a-b'), one('a_b'), one('init')],
});
