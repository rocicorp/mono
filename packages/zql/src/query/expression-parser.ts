import type {Condition} from '../../../zero-protocol/src/ast.js';

// This was written by ChatGPT. It is only used for tests

export function parse(input: string): Condition {
  const tokens = tokenize(input);
  const condition = parseOr(tokens);
  if (tokens.length > 0) {
    throw new Error('Unexpected input');
  }
  return condition;
}

function tokenize(input: string): string[] {
  return input.replace(/\s+/g, '').split('');
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

  return parseSimple(tokens);
}

function parseSimple(tokens: string[]): Condition {
  const token = tokens.shift();
  if (!token || !/^[a-zA-Z0-9]$/.test(token)) {
    throw new Error('Invalid input');
  }
  return {type: 'simple', value: token, op: '=', field: 'n/a'};
}

export function stringify(c: Condition): string {
  switch (c.type) {
    case 'simple':
      return (c.op === '!=' ? '!' : '') + c.value;
    case 'and':
    case 'or':
      return c.conditions
        .map(cond => {
          // Parentheses around "and" groups or nested "or" groups for clarity
          // and correctness. Also to catch unnecessary nesting.
          const needsParens =
            cond.type === 'and' ||
            (cond.type === 'or' && c.conditions.length > 1);
          return needsParens ? `(${stringify(cond)})` : stringify(cond);
        })
        .join(c.type === 'and' ? ' & ' : ' | ');
  }
}
