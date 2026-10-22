/**
 * Reads the TypeScript types of a schema's columns. Enumerations and typed
 * JSON columns exist only in the types (`enumeration<'a' | 'b'>()`,
 * `json<{…}>()`): at runtime the schema says `string` and `json`.
 */
import {dirname, resolve} from 'node:path';
import ts from 'typescript';

export type TypeModel =
  | {readonly kind: 'string' | 'number' | 'boolean' | 'json'}
  | {readonly kind: 'enum'; readonly alias?: string; readonly cases: string[]}
  | {
      readonly kind: 'object';
      readonly alias?: string;
      readonly fields: FieldModel[];
    }
  | {readonly kind: 'array'; readonly element: TypeModel}
  | {readonly kind: 'nullable'; readonly wrapped: TypeModel};

export type FieldModel = {
  readonly name: string;
  readonly type: TypeModel;
  /** Declared with `?`: may be left out. */
  readonly optional: boolean;
};

/** table → column → the model of its `customType`. */
export type CustomTypes = Map<string, Map<string, TypeModel>>;

const defaultOptions: ts.CompilerOptions = {
  target: ts.ScriptTarget.ES2022,
  module: ts.ModuleKind.NodeNext,
  moduleResolution: ts.ModuleResolutionKind.NodeNext,
  allowImportingTsExtensions: true,
  strict: true,
  noEmit: true,
  skipLibCheck: true,
};

export function readCustomTypes(
  schemaPath: string,
  exportName: string,
): CustomTypes {
  const file = resolve(schemaPath);
  const program = ts.createProgram([file], compilerOptions(file));
  const checker = program.getTypeChecker();
  const source = program.getSourceFile(file);
  const module = source && checker.getSymbolAtLocation(source);
  if (!module) {
    throw new Error(`Cannot read the types of ${schemaPath}`);
  }
  let symbol = checker
    .getExportsOfModule(module)
    .find(s => s.name === exportName);
  if (!symbol) {
    throw new Error(`${schemaPath} does not export "${exportName}"`);
  }
  if (symbol.flags & ts.SymbolFlags.Alias) {
    symbol = checker.getAliasedSymbol(symbol);
  }
  const schemaType = checker.getTypeOfSymbol(symbol);
  const out: CustomTypes = new Map();
  const tables = property(checker, schemaType, 'tables');
  for (const table of tables ? checker.getPropertiesOfType(tables) : []) {
    const columns = property(
      checker,
      checker.getTypeOfSymbol(table),
      'columns',
    );
    const byColumn = new Map<string, TypeModel>();
    for (const column of columns ? checker.getPropertiesOfType(columns) : []) {
      const custom = property(
        checker,
        checker.getTypeOfSymbol(column),
        'customType',
      );
      if (custom) {
        byColumn.set(column.name, model(checker, custom));
      }
    }
    out.set(table.name, byColumn);
  }
  return out;
}

function compilerOptions(file: string): ts.CompilerOptions {
  const configPath = ts.findConfigFile(dirname(file), ts.sys.fileExists);
  if (!configPath) {
    return defaultOptions;
  }
  const parsed = ts.getParsedCommandLineOfConfigFile(configPath, undefined, {
    ...ts.sys,
    onUnRecoverableConfigFileDiagnostic: () => {},
  });
  return parsed
    ? {...parsed.options, noEmit: true, skipLibCheck: true}
    : defaultOptions;
}

function property(
  checker: ts.TypeChecker,
  type: ts.Type,
  name: string,
): ts.Type | undefined {
  const p = checker.getPropertyOfType(type, name);
  return p && checker.getTypeOfSymbol(p);
}

/** How deep objects nest before the rest is `json`, for recursive generic
 * types, whose every level is a new type. */
const maxDepth = 32;

const nullish = ts.TypeFlags.Null | ts.TypeFlags.Undefined | ts.TypeFlags.Void;

/**
 * The model of a type. `outer` holds the object types it is inside of: a
 * recursive type, such as `type Tree = {children: Tree[]}`, is `json` where
 * it recurs.
 */
function model(
  checker: ts.TypeChecker,
  type: ts.Type,
  outer: ReadonlySet<ts.Type> = new Set(),
): TypeModel {
  if (outer.has(type) || outer.size >= maxDepth) {
    return {kind: 'json'};
  }
  const alias = aliasName(type);
  if (type.isUnion()) {
    const parts = type.types.filter(t => !(t.flags & nullish));
    const nullable = parts.length !== type.types.length;
    const inner = unionModel(checker, parts, alias, outer);
    return nullable ? {kind: 'nullable', wrapped: inner} : inner;
  }
  if (type.isIntersection()) {
    // Branded primitives, such as `string & {__brand: 'ID'}`.
    for (const t of type.types) {
      if (t.flags & ts.TypeFlags.StringLike) return {kind: 'string'};
      if (t.flags & ts.TypeFlags.NumberLike) return {kind: 'number'};
    }
    return {kind: 'json'};
  }
  if (type.isStringLiteral()) {
    return enumModel([type.value], alias);
  }
  if (type.flags & ts.TypeFlags.StringLike) return {kind: 'string'};
  if (type.flags & ts.TypeFlags.NumberLike) return {kind: 'number'};
  if (type.flags & ts.TypeFlags.BooleanLike) return {kind: 'boolean'};
  if (checker.isArrayType(type)) {
    const [element] = checker.getTypeArguments(type as ts.TypeReference);
    return {
      kind: 'array',
      element: element ? model(checker, element, outer) : {kind: 'json'},
    };
  }
  if (type.flags & ts.TypeFlags.Object && isPlainObject(checker, type)) {
    const inside = new Set(outer).add(type);
    const fields = checker.getPropertiesOfType(type).map(p => {
      const optional = (p.flags & ts.SymbolFlags.Optional) !== 0;
      let t = checker.getTypeOfSymbol(p);
      if (optional) {
        t = checker.getNonNullableType(t);
      }
      return {name: p.name, type: model(checker, t, inside), optional};
    });
    return {kind: 'object', ...(alias ? {alias} : {}), fields};
  }
  return {kind: 'json'};
}

function unionModel(
  checker: ts.TypeChecker,
  parts: ts.Type[],
  alias: string | undefined,
  outer: ReadonlySet<ts.Type>,
): TypeModel {
  if (parts.length === 0) return {kind: 'json'};
  if (parts.every(t => t.isStringLiteral())) {
    return enumModel(
      parts.map(t => (t as ts.StringLiteralType).value),
      alias,
    );
  }
  if (parts.every(t => t.flags & ts.TypeFlags.BooleanLike)) {
    return {kind: 'boolean'};
  }
  if (parts.every(t => t.flags & ts.TypeFlags.NumberLike)) {
    return {kind: 'number'};
  }
  if (parts.length === 1) return model(checker, parts[0], outer);
  return {kind: 'json'};
}

function enumModel(cases: string[], alias: string | undefined): TypeModel {
  return {kind: 'enum', ...(alias ? {alias} : {}), cases};
}

function isPlainObject(checker: ts.TypeChecker, type: ts.Type): boolean {
  return (
    checker.getPropertiesOfType(type).length > 0 &&
    type.getCallSignatures().length === 0 &&
    type.getConstructSignatures().length === 0 &&
    checker.getIndexInfosOfType(type).length === 0
  );
}

/** The name a type was declared under, when it was declared once by name. */
function aliasName(type: ts.Type): string | undefined {
  if (type.aliasSymbol && !type.aliasTypeArguments?.length) {
    return type.aliasSymbol.name;
  }
  const name = type.getSymbol()?.name;
  if (
    name &&
    !name.startsWith('__') &&
    type.flags & ts.TypeFlags.Object &&
    (type.getSymbol()!.flags & ts.SymbolFlags.Interface) !== 0
  ) {
    return name;
  }
  return undefined;
}
