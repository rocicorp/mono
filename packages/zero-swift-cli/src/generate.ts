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
  // The protocols the generated types conform to, and the types of their
  // members.
  'AllCases',
  'CaseIterable',
  'Codable',
  'CodingKey',
  'CodingKeys',
  'Decodable',
  'Encodable',
  'Equatable',
  'Hashable',
  'Hasher',
  'Identifiable',
  'KeyPath',
  'PartialKeyPath',
  'RawRepresentable',
  'RawValue',
  'Sendable',
]);

const swiftKeywords = new Set(
  (
    'associatedtype borrowing class consuming deinit enum extension fileprivate ' +
    'func import init inout internal let nonisolated open operator package ' +
    'private precedencegroup protocol public rethrows static struct subscript ' +
    'typealias var break case catch continue default defer do else ' +
    'fallthrough for guard if in repeat return throw switch where while Any ' +
    'as await false is nil self Self super throws true try _'
  ).split(' '),
);

/** Members Swift doesn't allow, even backticked. */
const unusableMembers = new Set(['self', 'Type', 'Protocol']);

/** The members of a row struct besides its columns and relationships. */
const rowMembers = [
  'tableName',
  'columns',
  'relationships',
  'columnValues',
  'hashValue',
  'CodingKeys',
  // The parameter of `init(entry:)`, which assigns the columns.
  'entry',
];

/** The members of a `json<T>()` object struct besides its fields. */
const objectMembers = [
  'zeroValue',
  'valueForNull',
  'hashValue',
  'CodingKeys',
  // The local of `init(zeroValue:)` and `zeroValue`, which use the fields.
  'o',
];

/** The members of an enumeration besides its cases. */
const enumMembers = [
  'rawValue',
  'RawValue',
  'zeroValue',
  'valueForNull',
  'allCases',
  'AllCases',
];

const asciiIdentifierChar = /[A-Za-z0-9_]/;
const unicodeIdentifierChar = /[\p{L}\p{M}\p{N}]/u;
const wordBoundary = /([a-z0-9])([A-Z])/g;

/** Code point ranges outside ASCII Swift allows in identifiers. */
const identifierRanges: readonly (readonly [number, number])[] = [
  [0xa8, 0xa8],
  [0xaa, 0xaa],
  [0xad, 0xad],
  [0xaf, 0xaf],
  [0xb2, 0xb5],
  [0xb7, 0xba],
  [0xbc, 0xbe],
  [0xc0, 0xd6],
  [0xd8, 0xf6],
  [0xf8, 0x2ff],
  [0x300, 0x36f],
  [0x370, 0x167f],
  [0x1681, 0x180d],
  [0x180f, 0x1dbf],
  [0x1dc0, 0x1dff],
  [0x1e00, 0x1fff],
  [0x2070, 0x218f],
  [0x2460, 0x24ff],
  [0x2776, 0x2793],
  [0x2c00, 0x2dff],
  [0x2e80, 0x2fff],
  [0x3004, 0x3007],
  [0x3021, 0x302f],
  [0x3031, 0x303f],
  [0x3040, 0xd7ff],
  [0xf900, 0xfd3d],
  [0xfd40, 0xfdcf],
  [0xfdf0, 0xfe1f],
  [0xfe20, 0xfe2f],
  [0xfe30, 0xfe44],
  [0xfe47, 0xfffd],
  [0x10000, 0xefffd],
];

/** Swift's identifier characters that can't start one: digits and combining marks. */
const nonHeadRanges: readonly (readonly [number, number])[] = [
  [0x30, 0x39],
  [0x300, 0x36f],
  [0x1dc0, 0x1dff],
  [0x20d0, 0x20ff],
  [0xfe20, 0xfe2f],
];

const inRanges = (c: number, ranges: readonly (readonly [number, number])[]) =>
  ranges.some(([lo, hi]) => c >= lo && c <= hi);

/** Whether Swift allows the character in an identifier: ASCII letters,
 * digits and `_`, and the letters, marks and digits of its Unicode ranges. */
function isIdentifierChar(ch: string): boolean {
  const c = ch.codePointAt(0)!;
  return c < 0x80
    ? asciiIdentifierChar.test(ch)
    : unicodeIdentifierChar.test(ch) && inRanges(c, identifierRanges);
}

function isIdentifierHead(ch: string): boolean {
  return isIdentifierChar(ch) && !inRanges(ch.codePointAt(0)!, nonHeadRanges);
}

/**
 * The name with the characters Swift doesn't allow in identifiers replaced
 * by `_`, and `_` before a leading digit. A name with none of them, such as
 * `+`, is `_` and its code points in hex: `_2B`. May be a keyword.
 */
function sanitize(name: string): string {
  const chars = [...name];
  if (!chars.some(isIdentifierChar)) {
    return chars.length
      ? '_' +
          chars
            .map(ch => ch.codePointAt(0)!.toString(16).toUpperCase())
            .join('_')
      : '_';
  }
  const id = chars.map(ch => (isIdentifierChar(ch) ? ch : '_'));
  return (isIdentifierHead(id[0]) ? '' : '_') + id.join('');
}

/** An identifier as code: backticked when it is a keyword. */
function ref(id: string): string {
  return swiftKeywords.has(id) ? '`' + id + '`' : id;
}

/** A name usable as a Swift identifier, backticked when it is a keyword. */
export function identifier(name: string): string {
  return ref(sanitize(name));
}

/**
 * The member names of one Swift type or scope. A name that is taken, or
 * that Swift doesn't allow as a member, gets `_` appended until it is
 * free.
 */
class Members {
  readonly #used: Set<string>;

  constructor(reserved: Iterable<string> = []) {
    this.#used = new Set(reserved);
  }

  #free(id: string): boolean {
    return !this.#used.has(id) && !unusableMembers.has(id);
  }

  #available(id: string, wrapped: boolean): boolean {
    return this.#free(id) && (!wrapped || this.#free('_' + id));
  }

  /**
   * A free member for `id`, not backticked. `wrapped` also claims the
   * `_id` storage of a property wrapper.
   */
  claim(id: string, wrapped = false): string {
    while (!this.#available(id, wrapped)) {
      id += '_';
    }
    this.#used.add(id);
    if (wrapped) {
      this.#used.add('_' + id);
    }
    return id;
  }

  /**
   * Claims a member for each name, from `id`, its identifier. The names
   * that are their identifier and free claim first, so a name that works
   * as it is keeps it.
   */
  claimAll(
    names: readonly {name: string; id: string; wrapped?: boolean}[],
  ): string[] {
    const out: string[] = [];
    const keep = names.map(
      n => n.id === n.name && this.#available(n.id, n.wrapped ?? false),
    );
    for (const pass of [true, false]) {
      for (const [i, n] of names.entries()) {
        if (keep[i] === pass) {
          out[i] = this.claim(n.id, n.wrapped);
        }
      }
    }
    return out;
  }
}

function words(name: string): string[] {
  const out: string[] = [];
  let word = '';
  for (const ch of name.replace(wordBoundary, '$1 $2')) {
    if (ch !== '_' && isIdentifierChar(ch)) {
      word += ch;
    } else if (word) {
      out.push(word);
      word = '';
    }
  }
  return word ? [...out, word] : out;
}

function capitalize(w: string): string {
  const [first, ...rest] = w;
  return first.toUpperCase() + rest.join('');
}

export function pascalCase(name: string): string {
  const out = words(name).map(capitalize).join('');
  if (out === '') {
    return sanitize(name);
  }
  return isIdentifierHead([...out][0]) ? out : '_' + out;
}

function camelCase(name: string): string {
  const p = pascalCase(name);
  if (p[0] === '_') {
    return p;
  }
  const [first, ...rest] = p;
  return first.toLowerCase() + rest.join('');
}

/** A Swift string literal. */
function str(s: string): string {
  let out = '"';
  for (const ch of s) {
    const c = ch.codePointAt(0)!;
    if (ch === '"' || ch === '\\') {
      out += '\\' + ch;
    } else if (ch === '\n') {
      out += '\\n';
    } else if (ch === '\r') {
      out += '\\r';
    } else if (ch === '\t') {
      out += '\\t';
    } else if (c < 0x20 || c === 0x7f || (c >= 0xd800 && c <= 0xdfff)) {
      // Swift has no `\b` or `\f`, and a lone surrogate can't be written.
      out += `\\u{${(c >= 0xd800 && c <= 0xdfff ? 0xfffd : c).toString(16)}}`;
    } else {
      out += ch;
    }
  }
  return out + '"';
}

/** A type name for the row or zql builder: free, and not a keyword. */
function typeTaken(name: string, used: ReadonlySet<string>): boolean {
  return (
    used.has(name) || reservedTypeNames.has(name) || swiftKeywords.has(name)
  );
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

  /**
   * A free type name, without the prefix: the alias, else `path` as an
   * identifier, else that with a number.
   */
  #claimBare(preferred: string | undefined, path: string): string {
    const fallback = sanitize(path);
    const free = (name: string) =>
      sanitize(name) === name && !typeTaken(this.#prefix + name, this.#names);
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
  /** The column's name in the schema. */
  name: string;
  /** The property, not backticked. */
  id: string;
  /** The property as code. */
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
  const schemaName = config.schemaName ?? 'app';
  const zqlName = config.zql ?? 'zql';
  const zqlType = zqlName === 'zql' ? 'ZQL' : pascalCase(zqlName);
  const rowNames = new Map<string, string>();
  const used = new Set<string>([zqlType]);
  for (const t of tableNames) {
    // A taken name gets `Row`, and then a number from 2.
    const preferred = config.tables?.[t]?.type ?? prefix + pascalCase(t);
    let name = preferred;
    for (let i = 1; typeTaken(name, used); i++) {
      name = `${preferred}Row${i === 1 ? '' : i}`;
    }
    used.add(name);
    rowNames.set(t, name);
  }
  const e = new Emitter(config.access ?? 'public', prefix, used);
  const bare = (name: string) =>
    prefix && name.startsWith(prefix) ? name.slice(prefix.length) : name;
  const pub = e.access;

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
      const id = sanitize(name);
      return {name, id, property: ref(id), swiftType, optional};
    });

    const relationships = Object.entries(schema.relationships[t] ?? {}).map(
      ([name, hops]) => {
        const last = hops.at(-1)!;
        const dest = rowNames.get(last.destSchema)!;
        const one = last.cardinality === 'one';
        const id = sanitize(name);
        return {name, id, property: ref(id), dest, one};
      },
    );
    // One scope: `@Indirect` also declares `_name`.
    const props = [...columns, ...relationships];
    const ids = new Members(rowMembers).claimAll(
      props.map(p => ({...p, wrapped: 'one' in p && p.one})),
    );
    for (const [i, p] of props.entries()) {
      p.id = ids[i];
      p.property = ref(ids[i]);
    }

    const identifiable =
      table.primaryKey.length === 1 &&
      table.primaryKey[0] === 'id' &&
      columns.some(c => c.name === 'id' && c.id === 'id' && !c.optional);
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
    lines.push(...codingKeys([...columns, ...relationships]));
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
  const tableIds = new Members().claimAll(
    tableNames.map(t => ({name: t, id: sanitize(t)})),
  );
  for (const [i, t] of tableNames.entries()) {
    const row = rowNames.get(t)!;
    out.push('');
    out.push(
      `  ${pub}var ${ref(tableIds[i])}: Query<${row}, [${row}]> { Query(AnyQuery(.${schemaName}, ${str(t)})) }`,
    );
  }
  out.push('}');
  out.push('');
  out.push(`${pub}let ${identifier(zqlName)} = ${zqlType}()`);
  out.push('');
  return out.join('\n');
}

/**
 * The `CodingKeys` of a struct whose properties aren't all named as their
 * keys, so `Codable` uses the names on the wire; none when they are.
 */
function codingKeys(
  props: readonly {name: string; id: string; property: string}[],
): string[] {
  if (props.every(p => p.id === p.name)) {
    return [];
  }
  return [
    '',
    '  private enum CodingKeys: String, CodingKey {',
    ...props.map(p =>
      p.id === p.name
        ? `    case ${p.property}`
        : `    case ${p.property} = ${str(p.name)}`,
    ),
    '  }',
  ];
}

function enumDecl(pub: string, n: {name: string; cases: string[]}): string {
  const ids = new Members(enumMembers).claimAll(
    n.cases.map(value => ({name: value, id: camelCase(value)})),
  );
  const lines = [
    `${pub}enum ${n.name}: String, ZeroEnum, Codable, CaseIterable {`,
  ];
  for (const [i, value] of n.cases.entries()) {
    const c = ids[i];
    lines.push(
      c === value ? `  case ${ref(c)}` : `  case ${ref(c)} = ${str(value)}`,
    );
  }
  lines.push('}');
  return lines.join('\n');
}

function objectDecl(
  e: Emitter,
  pub: string,
  n: {name: string; fields: FieldModel[]},
): string {
  const ids = new Members(objectMembers).claimAll(
    n.fields.map(f => ({name: f.name, id: sanitize(f.name)})),
  );
  const fields = n.fields.map((f, i) => {
    const base = e.swiftType(f.type, e.bare(n.name) + pascalCase(f.name));
    const type = f.optional && !base.endsWith('?') ? base + '?' : base;
    return {...f, id: ids[i], property: ref(ids[i]), swiftType: type};
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
  lines.push(...codingKeys(fields));
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
