import type {
  Condition,
  CorrelatedSubquery,
  SimpleOperator,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';

// This was written by ChatGPT with some improvements. It is only used for tests

export function parse(input: string): Condition {
  const tokens = tokenize(input);
  const condition = parseOr(tokens);
  if (tokens.length > 0) {
    throw new Error('Unexpected input');
  }
  return condition;
}

function tokenize(input: string): string[] {
  // Tokenize based on identifiers (letters/numbers), operators, and parentheses.
  const regex = /[a-zA-Z0-9]+|!=|<=|>=|[&|()!=<>]/g;
  return input.match(regex) ?? [];
}

function parseOr(tokens: string[]): Condition {
  const conditions: Condition[] = [];
  let current = parseAnd(tokens);

  while (tokens[0] === '|') {
    tokens.shift(); // consume '|'
    conditions.push(current);
    current = parseAnd(tokens);
  }

  conditions.push(current);

  return conditions.length === 1 ? conditions[0] : {type: 'or', conditions};
}

function parseAnd(tokens: string[]): Condition {
  const conditions: Condition[] = [];
  let current = parsePrimary(tokens);

  while (tokens[0] === '&') {
    tokens.shift(); // consume '&'
    conditions.push(current);
    current = parsePrimary(tokens);
  }

  conditions.push(current);

  return conditions.length === 1 ? conditions[0] : {type: 'and', conditions};
}

function parsePrimary(tokens: string[]): Condition {
  if (tokens[0] === '(') {
    tokens.shift(); // consume '('
    const condition = parseOr(tokens);
    if (tokens.shift() !== ')') {
      throw new Error('Missing closing parenthesis');
    }
    return condition;
  }

  return parseSimpleOrCorrelated(tokens);
}

function parseSimpleOrCorrelated(tokens: string[]): Condition {
  const token = tokens.shift();
  if (token === 'EXISTS') {
    eat(tokens, '(');
    // TODO: parse subquery
    eat(tokens, ')');
    return {
      type: 'correlatedSubquery',
      related: {} as CorrelatedSubquery,
      op: 'EXISTS',
    };
  } else if (token === 'NOT') {
    eat(tokens, 'EXISTS');
    eat(tokens, '(');
    // TODO: parse subquery
    eat(tokens, ')');
    return {
      type: 'correlatedSubquery',
      related: {} as CorrelatedSubquery,
      op: 'NOT EXISTS',
    };
  }

  if (!token || !/^[a-zA-Z0-9]+$/.test(token)) {
    throw new Error('Invalid input');
  }

  let maybeOp = tokens[0];
  if (
    maybeOp === 'NOT' &&
    (tokens[1] === 'IN' || tokens[1] === 'LIKE' || tokens[1] === 'ILIKE')
  ) {
    maybeOp += ' ' + tokens[1];
    tokens.shift();
  }

  if (simpleOperators.has(maybeOp)) {
    tokens.shift(); // consume operator
    const right = {
      type: 'literal',
      value: parseValue(tokens),
    } as const;
    return {
      type: 'simple',
      right,
      op: maybeOp as SimpleOperator,
      left: {
        type: 'column',
        name: token,
      },
    };
  }

  return {
    type: 'simple',
    right: {
      type: 'literal',
      value: token,
    },
    op: '=',
    left: {
      type: 'column',
      name: 'n/a',
    },
  };
}

const simpleOperators = new Set([
  '=',
  '!=',
  '<',
  '>',
  '>=',
  '<=',
  'IN',
  'NOT IN',
  'LIKE',
  'NOT LIKE',
  'ILIKE',
  'NOT ILIKE',
]);

function parseValue(tokens: string[]): string {
  const token = tokens.shift();
  if (!token || !/^[a-zA-Z0-9]+$/.test(token)) {
    throw new Error('Invalid input: ' + token);
  }
  return token;
}

export function stringify(c: Condition): string {
  switch (c.type) {
    case 'simple':
      if (c.left.type === 'column' && c.left.name === 'n/a') {
        return (c.op === '!=' ? '!' : '') + stringifyValue(c.right);
      }
      return `${stringifyValue(c.left)} ${c.op} ${stringifyValue(c.right)}`;
    case 'and':
    case 'or':
      return c.conditions
        .map(cond => {
          // Parentheses around "and" groups or nested "or" groups for clarity
          // and correctness. Also to catch unnecessary nesting.
          const needsParens = cond.type === 'and' || cond.type === 'or';
          return needsParens ? `(${stringify(cond)})` : stringify(cond);
        })
        .join(c.type === 'and' ? ' & ' : ' | ');
    case 'correlatedSubquery':
      return c.op + ' ()';
    case 'scalarSubquery':
      return `${c.parentField} ${c.op} (SCALAR)`;
  }
}
function eat(tokens: string[], expectedToken: string) {
  const token = tokens.shift();
  if (token !== expectedToken) {
    throw new Error(
      'Unexpected input. Got: ' + token + ' Expected: ' + expectedToken,
    );
  }
}

function stringifyValue(value: ValuePosition): string {
  switch (value.type) {
    case 'column':
      return value.name;
    case 'literal':
      return `${value.value}`;
    case 'static':
      return `@${value.anchor}.${value.field}`;
  }
}
