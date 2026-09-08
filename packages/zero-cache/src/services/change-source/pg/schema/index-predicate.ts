import * as PostgresTypeClass from '../../../../db/postgres-type-class-enum.ts';
import type {
  IndexPredicate,
  IndexPredicateValue,
  PublishedTableSpec,
} from '../../../../db/specs.ts';

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const INTEGER_RE = /^[+-]?\d+$/;
const WHITESPACE_RE = /\s/;
const OPERATOR_TOKEN_RE = /^(?:::|<>|!=|<=|>=|=|<|>)/;
const UNSUPPORTED_OPERATOR_TOKEN_RE = /^(?:!~\*|!~|~\*|~)/;
const INTEGER_TOKEN_RE = /^[+-]?\d+/;
const WORD_TOKEN_RE = /^[A-Za-z_][A-Za-z0-9_$]*/;
const SIMPLE_ESCAPES: Readonly<Record<string, string>> = {
  b: '\b',
  f: '\f',
  n: '\n',
  r: '\r',
  t: '\t',
  v: '\v',
};

export const unsupportedPredicateReasons = [
  'unpublished-column',
  'unsupported-type',
  'unsupported-operator',
  'unsupported-function',
  'unsupported-cast',
  'unsupported-syntax',
  'non-deterministic-collation',
] as const;

export type UnsupportedPredicateReason =
  (typeof unsupportedPredicateReasons)[number];

export type PredicateTranslation =
  | {readonly supported: true; readonly predicate: IndexPredicate}
  | {readonly supported: false; readonly reason: UnsupportedPredicateReason};

type PredicateColumnSpec = PublishedTableSpec['columns'][string] & {
  readonly collationIsDeterministic?: boolean | null | undefined;
};

type PredicateTableSpec = Omit<PublishedTableSpec, 'columns'> & {
  readonly columns: Readonly<Record<string, PredicateColumnSpec>>;
};

type Expr =
  | {readonly type: 'column'; readonly name: string}
  | {
      readonly type: 'literal';
      readonly value: boolean | string | null;
      readonly literalType: 'boolean' | 'integer' | 'string' | 'null';
    }
  | {readonly type: 'cast'; readonly expression: Expr; readonly cast: string}
  | {readonly type: 'not'; readonly expression: Expr}
  | {
      readonly type: 'and' | 'or';
      readonly expressions: readonly Expr[];
    }
  | {
      readonly type: 'comparison';
      readonly left: Expr;
      readonly op: '=' | '<>' | '!=' | '<' | '<=' | '>' | '>=';
      readonly right: Expr;
    }
  | {
      readonly type: 'null-test';
      readonly expression: Expr;
      readonly not: boolean;
    };

type Token =
  | {readonly type: 'identifier'; readonly value: string}
  | {
      readonly type: 'literal';
      readonly value: boolean | string | null;
      readonly literalType: 'boolean' | 'integer' | 'string' | 'null';
    }
  | {
      readonly type:
        | '('
        | ')'
        | '['
        | ']'
        | ','
        | '.'
        | '::'
        | '='
        | '<>'
        | '!='
        | '<'
        | '<='
        | '>'
        | '>='
        | 'AND'
        | 'OR'
        | 'NOT'
        | 'IN'
        | 'IS'
        | 'EOF';
    }
  | {readonly type: 'unsupported'; readonly value: string};

class UnsupportedPredicateError extends Error {
  readonly reason: UnsupportedPredicateReason;

  constructor(reason: UnsupportedPredicateReason) {
    super(reason);
    this.reason = reason;
  }
}

export function translatePostgresIndexPredicate(
  sql: string,
  table: PredicateTableSpec,
): PredicateTranslation {
  try {
    const parser = new Parser(tokenize(sql));
    const expression = parser.parse();
    parser.expect('EOF');
    return {supported: true, predicate: normalize(expression, table)};
  } catch (error) {
    return {
      supported: false,
      reason:
        error instanceof UnsupportedPredicateError
          ? error.reason
          : 'unsupported-syntax',
    };
  }
}

class Parser {
  readonly #tokens: readonly Token[];
  #position = 0;

  constructor(tokens: readonly Token[]) {
    this.#tokens = tokens;
  }

  parse(): Expr {
    return this.#parseOr();
  }

  #parseOr(): Expr {
    const expressions = [this.#parseAnd()];
    while (this.#take('OR')) expressions.push(this.#parseAnd());
    return expressions.length === 1
      ? expressions[0]
      : {type: 'or', expressions};
  }

  #parseAnd(): Expr {
    const expressions = [this.#parseNot()];
    while (this.#take('AND')) expressions.push(this.#parseNot());
    return expressions.length === 1
      ? expressions[0]
      : {type: 'and', expressions};
  }

  #parseNot(): Expr {
    if (this.#take('NOT')) {
      return {type: 'not', expression: this.#parseNot()};
    }
    return this.#parseComparison();
  }

  #parseComparison(): Expr {
    const left = this.#parseOperand();
    if (this.#take('IS')) {
      const not = this.#take('NOT');
      const token = this.#tokens[this.#position++];
      if (token?.type !== 'literal' || token.literalType !== 'null') {
        throw new UnsupportedPredicateError('unsupported-operator');
      }
      return {type: 'null-test', expression: left, not};
    }
    if (this.#take('IN')) {
      this.expect('(');
      const values = this.#parseOperandList(')');
      this.expect(')');
      return comparisons(left, values);
    }
    const op = this.#tokens[this.#position]?.type;
    if (isComparison(op)) {
      this.#position++;
      if (op === '=' && this.#takeIdentifier('ANY')) {
        return comparisons(left, this.#parseLiteralArray());
      }
      return {
        type: 'comparison',
        left,
        op,
        right: this.#parseOperand(),
      };
    }
    const next = this.#tokens[this.#position];
    if (next?.type === 'identifier' && next.value.toUpperCase() === 'COLLATE') {
      throw new UnsupportedPredicateError('non-deterministic-collation');
    }
    if (
      next?.type === 'unsupported' &&
      UNSUPPORTED_OPERATOR_TOKEN_RE.test(next.value)
    ) {
      throw new UnsupportedPredicateError('unsupported-operator');
    }
    if (
      next?.type === 'identifier' &&
      unsupportedOperatorWords.has(next.value.toUpperCase())
    ) {
      throw new UnsupportedPredicateError('unsupported-operator');
    }
    return left;
  }

  #parseLiteralArray(): readonly Expr[] {
    this.expect('(');
    this.#expectIdentifier('ARRAY');
    this.expect('[');
    const values = this.#parseOperandList(']');
    this.expect(']');
    if (this.#take('::')) {
      this.#parseCastName();
      this.expect('[');
      this.expect(']');
    }
    this.expect(')');
    return values;
  }

  #parseOperandList(end: ')' | ']'): readonly Expr[] {
    const values: Expr[] = [];
    if (this.#tokens[this.#position]?.type === end) return values;
    values.push(this.#parseOperand());
    while (this.#take(',')) values.push(this.#parseOperand());
    return values;
  }

  #parseOperand(): Expr {
    let expression = this.#parsePrimary();
    while (this.#take('::')) {
      expression = {
        type: 'cast',
        expression,
        cast: this.#parseCastName(),
      };
    }
    return expression;
  }

  #parsePrimary(): Expr {
    if (this.#take('(')) {
      const expression = this.#parseOr();
      this.expect(')');
      return expression;
    }
    const token = this.#tokens[this.#position++];
    if (token?.type === 'literal') {
      return {
        type: 'literal',
        value: token.value,
        literalType: token.literalType,
      };
    }
    if (token?.type === 'identifier') {
      if (this.#tokens[this.#position]?.type === '(') {
        throw new UnsupportedPredicateError('unsupported-function');
      }
      if (this.#tokens[this.#position]?.type === '.') {
        throw new UnsupportedPredicateError('unsupported-syntax');
      }
      if (token.value.toUpperCase() === 'COLLATE') {
        throw new UnsupportedPredicateError('non-deterministic-collation');
      }
      return {type: 'column', name: token.value};
    }
    throw new UnsupportedPredicateError('unsupported-syntax');
  }

  #parseCastName(): string {
    const first = this.#expectIdentifier();
    const parts = [first];
    let separator = '.';
    while (this.#take('.')) parts.push(this.#expectIdentifier());
    const next = this.#tokens[this.#position];
    if (
      first.toLowerCase() === 'character' &&
      next?.type === 'identifier' &&
      next.value.toLowerCase() === 'varying'
    ) {
      parts.push(this.#expectIdentifier());
      separator = ' ';
    }
    if (this.#tokens[this.#position]?.type === '(') {
      throw new UnsupportedPredicateError('unsupported-cast');
    }
    return parts.join(separator);
  }

  #expectIdentifier(expected?: string): string {
    const token = this.#tokens[this.#position++];
    if (
      token?.type !== 'identifier' ||
      (expected !== undefined && token.value.toUpperCase() !== expected)
    ) {
      throw new UnsupportedPredicateError('unsupported-cast');
    }
    return token.value;
  }

  #takeIdentifier(expected: string): boolean {
    const token = this.#tokens[this.#position];
    if (
      token?.type !== 'identifier' ||
      token.value.toUpperCase() !== expected
    ) {
      return false;
    }
    this.#position++;
    return true;
  }

  #take(type: Token['type']): boolean {
    if (this.#tokens[this.#position]?.type !== type) return false;
    this.#position++;
    return true;
  }

  expect(type: Token['type']): void {
    if (!this.#take(type)) {
      throw new UnsupportedPredicateError('unsupported-syntax');
    }
  }
}

const unsupportedOperatorWords = new Set([
  'ALL',
  'BETWEEN',
  'ILIKE',
  'LIKE',
  'SIMILAR',
]);

function comparisons(left: Expr, values: readonly Expr[]): Expr {
  if (values.length === 0) {
    throw new UnsupportedPredicateError('unsupported-syntax');
  }
  const expressions = values.map(
    right => ({type: 'comparison', left, op: '=', right}) as const,
  );
  return expressions.length === 1 ? expressions[0] : {type: 'or', expressions};
}

function normalize(
  expression: Expr,
  table: PredicateTableSpec,
): IndexPredicate {
  switch (expression.type) {
    case 'and':
    case 'or': {
      const conditions = expression.expressions.map(expr =>
        normalize(expr, table),
      );
      return {
        type: expression.type,
        conditions: conditions.flatMap(condition =>
          condition.type === expression.type
            ? condition.conditions
            : [condition],
        ),
      };
    }
    case 'not': {
      const booleanColumn = resolveBareBooleanColumn(
        expression.expression,
        table,
      );
      if (booleanColumn) {
        return booleanComparison(booleanColumn, false);
      }
      return {type: 'not', condition: normalize(expression.expression, table)};
    }
    case 'column': {
      const column = resolveColumn(expression, table);
      if (column.category !== 'boolean') {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return booleanComparison(column.name, true);
    }
    case 'cast': {
      const column = resolveColumnOperand(expression, table);
      if (column.category !== 'boolean') {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return booleanComparison(column.name, true);
    }
    case 'null-test': {
      const column = resolveNullTestColumn(expression.expression, table);
      return {
        type: 'null-test',
        column,
        op: expression.not ? 'IS NOT NULL' : 'IS NULL',
      };
    }
    case 'comparison':
      return normalizeComparison(expression, table);
    case 'literal':
      throw new UnsupportedPredicateError('unsupported-syntax');
  }
}

function resolveNullTestColumn(
  expression: Expr,
  table: PredicateTableSpec,
): string {
  if (expression.type === 'cast') {
    return resolveColumnOperand(expression, table).name;
  }
  if (expression.type === 'column' && !table.columns[expression.name]) {
    throw new UnsupportedPredicateError('unpublished-column');
  }
  if (expression.type === 'column') return expression.name;
  throw new UnsupportedPredicateError('unsupported-syntax');
}

function resolveBareBooleanColumn(
  expression: Expr,
  table: PredicateTableSpec,
): string | undefined {
  if (expression.type !== 'column' && expression.type !== 'cast') {
    return undefined;
  }
  const column = resolveColumnOperand(expression, table);
  return column.category === 'boolean' ? column.name : undefined;
}

function booleanComparison(column: string, value: boolean): IndexPredicate {
  return {
    type: 'comparison',
    column,
    op: '=',
    value: {type: 'boolean', value},
  };
}

type ColumnCategory =
  | {readonly category: 'boolean'; readonly name: string}
  | {
      readonly category: 'integer';
      readonly name: string;
      readonly rank: 0 | 1 | 2;
    }
  | {readonly category: 'text'; readonly name: string}
  | {
      readonly category: 'enum';
      readonly name: string;
      readonly enumName: string;
    }
  | {readonly category: 'uuid'; readonly name: string};

function normalizeComparison(
  expression: Extract<Expr, {type: 'comparison'}>,
  table: PredicateTableSpec,
): IndexPredicate {
  const column = resolveColumnOperand(expression.left, table);
  const op = expression.op === '!=' ? '<>' : expression.op;
  if (column.category !== 'integer' && op !== '=' && op !== '<>') {
    throw new UnsupportedPredicateError('unsupported-operator');
  }
  const value = resolveLiteral(expression.right, column);
  return {type: 'comparison', column: column.name, op, value};
}

function resolveColumnOperand(
  expression: Expr,
  table: PredicateTableSpec,
): ColumnCategory {
  if (expression.type === 'column') return resolveColumn(expression, table);
  if (expression.type !== 'cast') {
    throw new UnsupportedPredicateError('unsupported-syntax');
  }
  const column = resolveColumnOperand(expression.expression, table);
  const cast = canonicalTypeName(expression.cast);
  switch (column.category) {
    case 'integer': {
      const rank = integerRank(cast);
      if (rank === undefined || rank < column.rank) {
        throw new UnsupportedPredicateError('unsupported-cast');
      }
      return {...column, rank};
    }
    case 'text':
      if (cast !== 'text' && cast !== 'varchar') {
        throw new UnsupportedPredicateError('unsupported-cast');
      }
      return column;
    case 'boolean':
      if (cast !== 'bool') {
        throw new UnsupportedPredicateError('unsupported-cast');
      }
      return column;
    case 'uuid':
      if (cast !== 'uuid') {
        throw new UnsupportedPredicateError('unsupported-cast');
      }
      return column;
    case 'enum':
      if (lastTypeName(expression.cast) !== column.enumName.toLowerCase()) {
        throw new UnsupportedPredicateError('unsupported-cast');
      }
      return column;
  }
}

function resolveColumn(
  expression: Extract<Expr, {type: 'column'}>,
  table: PredicateTableSpec,
): ColumnCategory {
  const spec = table.columns[expression.name];
  if (!spec) throw new UnsupportedPredicateError('unpublished-column');
  const dataType = canonicalTypeName(spec.dataType);
  if (dataType === 'bool') return {category: 'boolean', name: expression.name};
  const rank = integerRank(dataType);
  if (rank !== undefined) {
    return {category: 'integer', name: expression.name, rank};
  }
  if (dataType === 'text' || dataType === 'varchar') {
    if (spec.collationIsDeterministic === false) {
      throw new UnsupportedPredicateError('non-deterministic-collation');
    }
    return {category: 'text', name: expression.name};
  }
  if (dataType === 'uuid') return {category: 'uuid', name: expression.name};
  if (spec.pgTypeClass === PostgresTypeClass.Enum) {
    return {
      category: 'enum',
      name: expression.name,
      enumName: spec.dataType.toLowerCase(),
    };
  }
  throw new UnsupportedPredicateError('unsupported-type');
}

function resolveLiteral(
  expression: Expr,
  column: ColumnCategory,
): IndexPredicateValue {
  let literal = expression;
  let cast: string | undefined;
  while (literal.type === 'cast') {
    cast = literal.cast;
    literal = literal.expression;
  }
  if (literal.type !== 'literal' || literal.value === null) {
    throw new UnsupportedPredicateError('unsupported-syntax');
  }

  switch (column.category) {
    case 'boolean': {
      validateLiteralCast(cast, new Set(['bool']));
      if (literal.literalType !== 'boolean') {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return {type: 'boolean', value: literal.value as boolean};
    }
    case 'integer': {
      if (cast !== undefined) {
        const castRank = integerRank(canonicalTypeName(cast));
        if (castRank === undefined || castRank > column.rank) {
          throw new UnsupportedPredicateError('unsupported-cast');
        }
      }
      if (
        literal.literalType !== 'integer' &&
        !(literal.literalType === 'string' && cast !== undefined)
      ) {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return {
        type: 'integer',
        value: canonicalInteger(String(literal.value), column.rank),
      };
    }
    case 'text':
      validateLiteralCast(cast, new Set(['text', 'varchar']));
      if (literal.literalType !== 'string') {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return {type: 'string', value: String(literal.value)};
    case 'enum':
      if (
        cast !== undefined &&
        lastTypeName(cast) !== column.enumName.toLowerCase()
      ) {
        throw new UnsupportedPredicateError('unsupported-cast');
      }
      if (literal.literalType !== 'string') {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return {type: 'string', value: String(literal.value)};
    case 'uuid': {
      validateLiteralCast(cast, new Set(['uuid']));
      const value = String(literal.value).toLowerCase();
      if (literal.literalType !== 'string' || !UUID_RE.test(value)) {
        throw new UnsupportedPredicateError('unsupported-type');
      }
      return {type: 'string', value};
    }
  }
}

function validateLiteralCast(
  cast: string | undefined,
  allowed: ReadonlySet<string>,
): void {
  if (cast !== undefined && !allowed.has(canonicalTypeName(cast))) {
    throw new UnsupportedPredicateError('unsupported-cast');
  }
}

function canonicalInteger(value: string, rank: 0 | 1 | 2): string {
  if (!INTEGER_RE.test(value)) {
    throw new UnsupportedPredicateError('unsupported-type');
  }
  let integer: bigint;
  try {
    integer = BigInt(value);
  } catch {
    throw new UnsupportedPredicateError('unsupported-type');
  }
  const bounds = [
    [-32768n, 32767n],
    [-2147483648n, 2147483647n],
    [-9223372036854775808n, 9223372036854775807n],
  ] as const;
  if (integer < bounds[rank][0] || integer > bounds[rank][1]) {
    throw new UnsupportedPredicateError('unsupported-type');
  }
  return integer.toString();
}

function canonicalTypeName(type: string): string {
  const name = lastTypeName(type).replace(/\s+/g, ' ');
  switch (name) {
    case 'boolean':
      return 'bool';
    case 'smallint':
      return 'int2';
    case 'integer':
      return 'int4';
    case 'bigint':
      return 'int8';
    case 'character varying':
      return 'varchar';
    default:
      return name;
  }
}

function lastTypeName(type: string): string {
  return type.split('.').at(-1)?.toLowerCase() ?? '';
}

function integerRank(type: string): 0 | 1 | 2 | undefined {
  switch (type) {
    case 'int2':
      return 0;
    case 'int4':
      return 1;
    case 'int8':
      return 2;
    default:
      return undefined;
  }
}

function isComparison(
  type: Token['type'] | undefined,
): type is '=' | '<>' | '!=' | '<' | '<=' | '>' | '>=' {
  return (
    type === '=' ||
    type === '<>' ||
    type === '!=' ||
    type === '<' ||
    type === '<=' ||
    type === '>' ||
    type === '>='
  );
}

function tokenize(sql: string): readonly Token[] {
  const tokens: Token[] = [];
  for (let i = 0; i < sql.length;) {
    const char = sql[i];
    if (WHITESPACE_RE.test(char)) {
      i++;
      continue;
    }
    if (char === '"') {
      const result = readQuoted(sql, i, '"', false);
      tokens.push({type: 'identifier', value: result.value});
      i = result.end;
      continue;
    }
    const escapeString = (char === 'E' || char === 'e') && sql[i + 1] === "'";
    if (char === "'" || escapeString) {
      const result = readQuoted(
        sql,
        i + (escapeString ? 1 : 0),
        "'",
        escapeString,
      );
      tokens.push({
        type: 'literal',
        literalType: 'string',
        value: result.value,
      });
      i = result.end;
      continue;
    }
    const operator = OPERATOR_TOKEN_RE.exec(sql.slice(i))?.[0];
    if (operator) {
      tokens.push({
        type: operator as '::' | '=' | '<>' | '!=' | '<' | '<=' | '>' | '>=',
      });
      i += operator.length;
      continue;
    }
    const unsupportedOperator = UNSUPPORTED_OPERATOR_TOKEN_RE.exec(
      sql.slice(i),
    )?.[0];
    if (unsupportedOperator) {
      tokens.push({type: 'unsupported', value: unsupportedOperator});
      i += unsupportedOperator.length;
      continue;
    }
    if (
      char === '(' ||
      char === ')' ||
      char === '[' ||
      char === ']' ||
      char === ',' ||
      char === '.'
    ) {
      tokens.push({type: char});
      i++;
      continue;
    }
    const integer = INTEGER_TOKEN_RE.exec(sql.slice(i))?.[0];
    if (integer) {
      tokens.push({type: 'literal', literalType: 'integer', value: integer});
      i += integer.length;
      continue;
    }
    const word = WORD_TOKEN_RE.exec(sql.slice(i))?.[0];
    if (word) {
      const keyword = word.toUpperCase();
      if (unsupportedOperatorWords.has(keyword)) {
        throw new UnsupportedPredicateError('unsupported-operator');
      }
      if (keyword === 'TRUE' || keyword === 'FALSE') {
        tokens.push({
          type: 'literal',
          literalType: 'boolean',
          value: keyword === 'TRUE',
        });
      } else if (keyword === 'NULL') {
        tokens.push({type: 'literal', literalType: 'null', value: null});
      } else if (
        keyword === 'AND' ||
        keyword === 'OR' ||
        keyword === 'NOT' ||
        keyword === 'IN' ||
        keyword === 'IS'
      ) {
        tokens.push({type: keyword});
      } else {
        tokens.push({type: 'identifier', value: word.toLowerCase()});
      }
      i += word.length;
      continue;
    }
    tokens.push({type: 'unsupported', value: char});
    i++;
  }
  tokens.push({type: 'EOF'});
  return tokens;
}

function readQuoted(
  sql: string,
  start: number,
  quote: '"' | "'",
  escapes: boolean,
): {readonly value: string; readonly end: number} {
  let value = '';
  for (let i = start + 1; i < sql.length; i++) {
    const char = sql[i];
    if (char === quote) {
      if (sql[i + 1] === quote) {
        value += quote;
        i++;
      } else {
        return {value, end: i + 1};
      }
    } else if (escapes && char === '\\') {
      const escape = sql[++i];
      if (escape === undefined) {
        throw new UnsupportedPredicateError('unsupported-syntax');
      }
      if (escape === 'x') {
        const hex = readEscapeDigits(sql, i + 1, 2, 16, 1);
        value += String.fromCharCode(hex.value);
        i = hex.end - 1;
      } else if (escape === 'u' || escape === 'U') {
        const hex = readEscapeDigits(sql, i + 1, escape === 'u' ? 4 : 8, 16);
        try {
          value += String.fromCodePoint(hex.value);
        } catch {
          throw new UnsupportedPredicateError('unsupported-syntax');
        }
        i = hex.end - 1;
      } else if (escape >= '0' && escape <= '7') {
        let octal = escape;
        while (
          octal.length < 3 &&
          sql[i + 1] !== undefined &&
          sql[i + 1] >= '0' &&
          sql[i + 1] <= '7'
        ) {
          octal += sql[++i];
        }
        value += String.fromCharCode(Number.parseInt(octal, 8));
      } else if (escape === '\n') {
        // A backslash-newline pair is a line continuation.
      } else if (escape === '\r' && sql[i + 1] === '\n') {
        i++;
      } else {
        value += SIMPLE_ESCAPES[escape] ?? escape;
      }
    } else {
      value += char;
    }
  }
  throw new UnsupportedPredicateError('unsupported-syntax');
}

function readEscapeDigits(
  sql: string,
  start: number,
  length: number,
  radix: 16,
  minLength = length,
): {readonly value: number; readonly end: number} {
  let end = start;
  while (end < start + length) {
    const digit = sql[end];
    if (
      digit === undefined ||
      !(
        (digit >= '0' && digit <= '9') ||
        (digit.toLowerCase() >= 'a' && digit.toLowerCase() <= 'f')
      )
    ) {
      break;
    }
    end++;
  }
  const digits = sql.slice(start, end);
  if (digits.length < minLength) {
    throw new UnsupportedPredicateError('unsupported-syntax');
  }
  return {value: Number.parseInt(digits, radix), end};
}
