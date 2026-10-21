/**
 * Emits the Swift for a Zero schema: a row struct per table, the types of
 * its enumerations and typed JSON columns, the runtime schema, and the
 * `zql` query builder.
 */
import type {Schema} from '../../zero-types/src/schema.ts';
import type {CustomTypes, FieldModel, TypeModel} from './types.ts';

export type Config = {
  /** The TS schema module. Default `schema.ts`. */
  readonly schema?: string | undefined;
  /** The export of the schema module that holds the schema. Default `schema`. */
  readonly export?: string | undefined;
  /** The Swift file to write. Default `Schema.swift`. */
  readonly output?: string | undefined;
  /** The `ZeroSchema` static the schema is generated as. Default `app`. */
  readonly schemaName?: string | undefined;
  /** The global query builder. Default `zql`, of type `ZQL`. */
  readonly zql?: string | undefined;
  /** Prepended to every generated type name. Default none. */
  readonly typePrefix?: string | undefined;
  /** The access level of everything generated. Default `public`. */
  readonly access?: 'public' | 'internal' | undefined;
  readonly tables?:
    | Readonly<
        Record<
          string,
          {
            /** The row struct's name. Default the table name in PascalCase. */
            readonly type?: string | undefined;
            readonly columns?:
              | Readonly<
                  Record<string, {readonly type?: 'int' | 'double' | undefined}>
                >
              | undefined;
          }
        >
      >
    | undefined;
};

/** Zero types the generated code names unqualified. */
const reservedTypeNames = new Set([
  'AnyQuery',
  'Cardinality',
  'ColumnSchema',
  'Column',
  'Condition',
  'Format',
  'Indirect',
  'JSONValue',
  'KeyPathNames',
  'Query',
  'QueryResult',
  'RelationshipHop',
  'Row',
  'RowExpression',
  'TableSchema',
  'ViewEntry',
  'ViewValue',
  'Zero',
  'ZeroEnum',
  'ZeroObjectReader',
  'ZeroObjectWriter',
  'ZeroRow',
  'ZeroSchema',
  'ZeroValue',
  'ZeroValueError',
  'Array',
  'Bool',
  'Dictionary',
  'Double',
  'Int',
  'Optional',
  'Set',
  'String',
]);

const swiftKeywords = new Set(
  (
    'associatedtype borrowing class consuming deinit enum extension fileprivate ' +
    'func import init inout internal let nonisolated open operator package ' +
    'private precedencegroup protocol public rethrows static struct subscript ' +
    'typealias var break case catch continue default defer do else ' +
    'fallthrough for guard if in repeat return throw switch where while Any ' +
    'as await false is nil self Self super throws true try'
  ).split(' '),
);

const nonIdentifierChars = /[^A-Za-z0-9_]/g;
const leadingDigit = /^[0-9]/;
const wordBoundary = /([a-z0-9])([A-Z])/g;
const wordSeparators = /[^A-Za-z0-9]+/;
const identifierStart = /^[A-Za-z_]/;

/** A name usable as a Swift identifier, backticked when it is a keyword. */
export function identifier(name: string): string {
  let id = name.replace(nonIdentifierChars, '_');
  if (leadingDigit.test(id) || id === '') {
    id = '_' + id;
  }
  return swiftKeywords.has(id) ? '`' + id + '`' : id;
}

function words(name: string): string[] {
  return name
    .replace(wordBoundary, '$1 $2')
    .split(wordSeparators)
    .filter(w => w !== '');
}

export function pascalCase(name: string): string {
  const out = words(name)
    .map(w => w[0].toUpperCase() + w.slice(1))
    .join('');
  return identifierStart.test(out) ? out : '_' + out;
}

function camelCase(name: string): string {
  const p = pascalCase(name);
  return p[0] === '_' ? p : p[0].toLowerCase() + p.slice(1);
}

function str(s: string): string {
  return JSON.stringify(s);
}

function strings(a: readonly string[]): string {
  return `[${a.map(str).join(', ')}]`;
}

type Named =
  | {kind: 'enum'; name: string; cases: string[]}
  | {kind: 'object'; name: string; fields: FieldModel[]};

class Emitter {
  readonly #access: string;
  readonly #named: Named[] = [];
  readonly #names = new Set<string>();
  /** Declared types by alias and shape, so a type used twice is one type. */
  readonly #byAlias = new Map<string, string>();

  readonly #prefix: string;

  constructor(access: string, prefix: string, rowNames: Iterable<string>) {
    this.#access = access === 'internal' ? '' : 'public ';
    this.#prefix = prefix;
    for (const n of rowNames) {
      this.#names.add(n);
    }
  }

  get access() {
    return this.#access;
  }

  bare(name: string): string {
    return this.#prefix && name.startsWith(this.#prefix)
      ? name.slice(this.#prefix.length)
      : name;
  }

  get named(): readonly Named[] {
    return this.#named;
  }

  /** A free type name, without the prefix: the alias, else `fallback`. */
  #claimBare(preferred: string | undefined, fallback: string): string {
    const free = (name: string) =>
      !this.#names.has(this.#prefix + name) &&
      !reservedTypeNames.has(this.#prefix + name);
    for (const name of [preferred, fallback]) {
      if (name && free(name)) {
        this.#names.add(this.#prefix + name);
        return name;
      }
    }
    for (let i = 2; ; i++) {
      const name = `${fallback}${i}`;
      if (free(name)) {
        this.#names.add(this.#prefix + name);
        return name;
      }
    }
  }

  /** The Swift type of a model, naming its enums and objects after `path`. */
  swiftType(model: TypeModel, path: string): string {
    switch (model.kind) {
      case 'string':
        return 'String';
      case 'number':
        return 'Double';
      case 'boolean':
        return 'Bool';
      case 'json':
        return 'JSONValue';
      case 'nullable':
        return `${this.swiftType(model.wrapped, path)}?`;
      case 'array':
        return `[${this.swiftType(model.element, path)}]`;
      case 'enum':
      case 'object': {
        const key = model.alias && `${model.alias}:${JSON.stringify(model)}`;
        const existing = key && this.#byAlias.get(key);
        if (existing) {
          return existing;
        }
        const name = this.#prefix + this.#claimBare(model.alias, path);
        if (key) {
          this.#byAlias.set(key, name);
        }
        this.#named.push(
          model.kind === 'enum'
            ? {kind: 'enum', name, cases: model.cases}
            : {kind: 'object', name, fields: model.fields},
        );
        return name;
      }
    }
  }
}

type Column = {
  name: string;
  property: string;
  swiftType: string;
  optional: boolean;
};

export function generate(
  schema: Schema,
  customTypes: CustomTypes,
  config: Config,
  source: string,
): string {
  const tableNames = Object.keys(schema.tables);
  const prefix = config.typePrefix ?? '';
  const rowNames = new Map<string, string>();
  const used = new Set<string>();
  for (const t of tableNames) {
    let name = config.tables?.[t]?.type ?? prefix + pascalCase(t);
    if (reservedTypeNames.has(name) || used.has(name)) {
      name += 'Row';
    }
    used.add(name);
    rowNames.set(t, name);
  }
  const e = new Emitter(config.access ?? 'public', prefix, used);
  const bare = (name: string) =>
    prefix && name.startsWith(prefix) ? name.slice(prefix.length) : name;
  const pub = e.access;
  const schemaName = config.schemaName ?? 'app';
  const zqlName = config.zql ?? 'zql';
  const zqlType = zqlName === 'zql' ? 'ZQL' : pascalCase(zqlName);

  const out: string[] = [
    `// Generated by \`zero-swift codegen\` from ${source}. Do not edit.`,
    '// swift-format-ignore-file',
    '',
    'import Zero',
    '',
  ];

  const typeDecls: string[] = [];
  for (const t of tableNames) {
    const table = schema.tables[t];
    const row = rowNames.get(t)!;
    const columns: Column[] = Object.entries(table.columns).map(([name, c]) => {
      const custom = customTypes.get(t)?.get(name);
      const override = config.tables?.[t]?.columns?.[name]?.type;
      let base: string;
      if (override === 'int') {
        base = 'Int';
      } else if (c.type === 'number' || override === 'double') {
        base = 'Double';
      } else if (c.type === 'boolean') {
        base = 'Bool';
      } else if (c.type === 'string') {
        base =
          custom?.kind === 'enum'
            ? e.swiftType(custom, bare(row) + pascalCase(name))
            : 'String';
      } else if (c.type === 'json') {
        base = custom
          ? e.swiftType(custom, bare(row) + pascalCase(name))
          : 'JSONValue';
      } else {
        base = 'JSONValue';
      }
      const optional = c.optional === true;
      const swiftType = optional && !base.endsWith('?') ? base + '?' : base;
      return {name, property: identifier(name), swiftType, optional};
    });

    const relationships = Object.entries(schema.relationships[t] ?? {}).map(
      ([name, hops]) => {
        const last = hops.at(-1)!;
        const dest = rowNames.get(last.destSchema)!;
        return {
          name,
          property: identifier(name),
          dest,
          one: last.cardinality === 'one',
        };
      },
    );

    const identifiable =
      table.primaryKey.length === 1 &&
      table.primaryKey[0] === 'id' &&
      columns.some(c => c.name === 'id' && !c.optional);
    const conformances = ['ZeroRow', 'Codable', 'Hashable'];
    if (identifiable) {
      conformances.push('Identifiable');
    }

    const lines: string[] = [];
    lines.push(`${pub}struct ${row}: ${conformances.join(', ')} {`);
    for (const c of columns) {
      lines.push(`  ${pub}var ${c.property}: ${c.swiftType}`);
    }
    if (relationships.length) {
      lines.push('');
      lines.push(
        '  // Relationships, filled when a query asks for them with `related`.',
      );
      for (const r of relationships) {
        lines.push(
          r.one
            ? `  @Indirect ${pub}var ${r.property}: ${r.dest}?`
            : `  ${pub}var ${r.property}: [${r.dest}]?`,
        );
      }
    }

    const params = [
      ...columns.map(
        c => `${c.property}: ${c.swiftType}${c.optional ? ' = nil' : ''}`,
      ),
      ...relationships.map(
        r => `${r.property}: ${r.one ? `${r.dest}?` : `[${r.dest}]?`} = nil`,
      ),
    ];
    lines.push('');
    lines.push(`  ${pub}init(`);
    lines.push(params.map(p => `    ${p}`).join(',\n'));
    lines.push('  ) {');
    for (const c of [...columns, ...relationships]) {
      lines.push(`    self.${c.property} = ${c.property}`);
    }
    lines.push('  }');

    lines.push('');
    lines.push(`  ${pub}static let tableName = ${str(t)}`);
    lines.push('');
    lines.push(`  ${pub}static let columns: KeyPathNames<${row}> = [`);
    for (const c of columns) {
      lines.push(`    \\${row}.${c.property}: ${str(c.name)},`);
    }
    lines.push('  ]');
    lines.push('');
    lines.push(
      `  ${pub}static let relationships: KeyPathNames<${row}> = ${
        relationships.length ? '[' : '[:]'
      }`,
    );
    if (relationships.length) {
      for (const r of relationships) {
        lines.push(`    \\${row}.${r.property}: ${str(r.name)},`);
      }
      lines.push('  ]');
    }

    lines.push('');
    lines.push(`  ${pub}init(entry: ViewEntry) throws(ZeroValueError) {`);
    // The index is the column's position in the table, where the rows have
    // it: reading it there skips looking the name up.
    for (const [i, c] of columns.entries()) {
      lines.push(`    ${c.property} = try entry.column(${str(c.name)}, ${i})`);
    }
    for (const r of relationships) {
      lines.push(`    ${r.property} = try entry.relationship(${str(r.name)})`);
    }
    lines.push('  }');

    lines.push('');
    lines.push(`  ${pub}var columnValues: Row {`);
    lines.push('    [');
    for (const c of columns) {
      lines.push(`      ${str(c.name)}: ${c.property}.zeroValue,`);
    }
    lines.push('    ]');
    lines.push('  }');
    lines.push('}');
    typeDecls.push(lines.join('\n'));
  }

  // Named types are discovered while emitting the rows, and object fields
  // can name more of them.
  const namedDecls: string[] = [];
  for (let i = 0; i < e.named.length; i++) {
    const n = e.named[i];
    namedDecls.push(
      n.kind === 'enum' ? enumDecl(pub, n) : objectDecl(e, pub, n),
    );
  }

  out.push(...typeDecls.flatMap(d => [d, '']));
  out.push(...namedDecls.flatMap(d => [d, '']));
  out.push(schemaDecl(pub, schema, schemaName));
  out.push('');
  out.push(
    `/// The queries of \`ZeroSchema.${schemaName}\`, one per table: \`${zqlName}.${tableNames[0] ?? 'table'}.where(…)\`.`,
  );
  out.push(`${pub}struct ${zqlType}: Sendable {`);
  out.push(`  ${pub}init() {}`);
  for (const t of tableNames) {
    const row = rowNames.get(t)!;
    out.push('');
    out.push(
      `  ${pub}var ${identifier(t)}: Query<${row}, [${row}]> { Query(AnyQuery(.${schemaName}, ${str(t)})) }`,
    );
  }
  out.push('}');
  out.push('');
  out.push(`${pub}let ${identifier(zqlName)} = ${zqlType}()`);
  out.push('');
  return out.join('\n');
}

function enumDecl(pub: string, n: {name: string; cases: string[]}): string {
  const used = new Set<string>();
  const lines = [
    `${pub}enum ${n.name}: String, ZeroEnum, Codable, CaseIterable {`,
  ];
  for (const value of n.cases) {
    let c = camelCase(value);
    while (used.has(c)) {
      c += '_';
    }
    used.add(c);
    const id = identifier(c);
    lines.push(c === value ? `  case ${id}` : `  case ${id} = ${str(value)}`);
  }
  lines.push('}');
  return lines.join('\n');
}

function objectDecl(
  e: Emitter,
  pub: string,
  n: {name: string; fields: FieldModel[]},
): string {
  const fields = n.fields.map(f => {
    const base = e.swiftType(f.type, e.bare(n.name) + pascalCase(f.name));
    const type = f.optional && !base.endsWith('?') ? base + '?' : base;
    return {...f, property: identifier(f.name), swiftType: type};
  });
  const lines = [`${pub}struct ${n.name}: ZeroValue, Codable, Hashable {`];
  for (const f of fields) {
    lines.push(`  ${pub}var ${f.property}: ${f.swiftType}`);
  }
  lines.push('');
  lines.push(`  ${pub}init(`);
  lines.push(
    fields
      .map(
        f =>
          `    ${f.property}: ${f.swiftType}${f.swiftType.endsWith('?') ? ' = nil' : ''}`,
      )
      .join(',\n'),
  );
  lines.push('  ) {');
  for (const f of fields) {
    lines.push(`    self.${f.property} = ${f.property}`);
  }
  lines.push('  }');
  lines.push('');
  lines.push(`  ${pub}init(zeroValue: JSONValue) throws(ZeroValueError) {`);
  lines.push(`    let o = try ZeroObjectReader(zeroValue, ${str(n.name)})`);
  for (const f of fields) {
    lines.push(`    ${f.property} = try o.field(${str(f.name)})`);
  }
  lines.push('  }');
  lines.push('');
  lines.push(`  ${pub}var zeroValue: JSONValue {`);
  lines.push('    var o = ZeroObjectWriter()');
  for (const f of fields) {
    lines.push(
      f.optional
        ? `    o.setIfPresent(${str(f.name)}, ${f.property})`
        : `    o.set(${str(f.name)}, ${f.property})`,
    );
  }
  lines.push('    return o.value');
  lines.push('  }');
  lines.push('}');
  return lines.join('\n');
}

function schemaDecl(pub: string, schema: Schema, name: string): string {
  const lines = ['extension ZeroSchema {'];
  lines.push(`  ${pub}static let ${identifier(name)} = ZeroSchema(`);
  lines.push('    tables: [');
  for (const [t, table] of Object.entries(schema.tables)) {
    lines.push('      TableSchema(');
    lines.push(
      `        name: ${str(t)},${table.serverName ? ` serverName: ${str(table.serverName)},` : ''}`,
    );
    lines.push('        columns: [');
    for (const [c, col] of Object.entries(table.columns)) {
      const args = [`.${col.type}`];
      if (col.optional) args.push('optional: true');
      if (col.serverName) args.push(`serverName: ${str(col.serverName)}`);
      lines.push(`          (${str(c)}, ColumnSchema(${args.join(', ')})),`);
    }
    lines.push('        ],');
    const keys = table.uniqueKeys?.length
      ? `,\n        uniqueKeys: [${table.uniqueKeys.map(strings).join(', ')}]`
      : '';
    lines.push(`        primaryKey: ${strings(table.primaryKey)}${keys}),`);
  }
  lines.push('    ],');
  const rels = Object.entries(schema.relationships).filter(
    ([, r]) => Object.keys(r).length > 0,
  );
  if (rels.length === 0) {
    lines.push('    relationships: [:])');
  } else {
    lines.push('    relationships: [');
    for (const [t, byName] of rels) {
      lines.push(`      ${str(t)}: [`);
      for (const [r, hops] of Object.entries(byName)) {
        lines.push(`        ${str(r)}: [`);
        for (const h of hops) {
          lines.push(
            `          RelationshipHop(sourceField: ${strings(h.sourceField)}, destField: ${strings(h.destField)}, destSchema: ${str(h.destSchema)}, cardinality: .${h.cardinality}),`,
          );
        }
        lines.push('        ],');
      }
      lines.push('      ],');
    }
    lines.push('    ])');
  }
  lines.push('}');
  return lines.join('\n');
}
