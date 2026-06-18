// oxlint-disable e18e/prefer-static-regex

export type SqlStatement = {
  file?: string | undefined;
  sql: string;
  startLine: number;
  endLine: number;
  index: number;
};

export function splitSqlStatements(
  input: string,
  file?: string,
): SqlStatement[] {
  const statements: SqlStatement[] = [];
  let start = 0;
  let startLine = 1;
  let line = 1;
  let i = 0;

  while (i < input.length) {
    const skipped = skipQuotedOrComment(input, i);
    if (skipped !== undefined) {
      line += countNewlines(input, i, skipped);
      i = skipped;
      continue;
    }

    const ch = input[i];
    if (ch === ';') {
      const sql = input.slice(start, i).trim();
      if (sql) {
        statements.push({
          file,
          sql,
          startLine,
          endLine: line,
          index: statements.length,
        });
      }
      i++;
      start = i;
      startLine = line;
      continue;
    }
    if (ch === '\n') {
      line++;
    }
    i++;
  }

  const sql = input.slice(start).trim();
  if (sql) {
    statements.push({
      file,
      sql,
      startLine,
      endLine: line,
      index: statements.length,
    });
  }

  return statements;
}

export function stripSqlComments(input: string): string {
  let out = '';
  let i = 0;
  while (i < input.length) {
    const next = input[i + 1];
    if (input[i] === '-' && next === '-') {
      const end = input.indexOf('\n', i + 2);
      if (end === -1) {
        return out;
      }
      out += '\n';
      i = end + 1;
      continue;
    }
    if (input[i] === '/' && next === '*') {
      const end = input.indexOf('*/', i + 2);
      const blockEnd = end === -1 ? input.length : end + 2;
      out += input.slice(i, blockEnd).replace(/[^\n]/g, ' ');
      i = blockEnd;
      continue;
    }

    const skipped = skipQuotedOrComment(input, i);
    if (skipped !== undefined) {
      out += input.slice(i, skipped);
      i = skipped;
      continue;
    }

    out += input[i];
    i++;
  }
  return out;
}

export function normalizeSql(input: string): string {
  return stripSqlComments(input).replace(/\s+/g, ' ').trim();
}

export function splitTopLevel(input: string, separator: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let depth = 0;
  let i = 0;
  while (i < input.length) {
    const skipped = skipQuotedOrComment(input, i);
    if (skipped !== undefined) {
      i = skipped;
      continue;
    }
    const ch = input[i];
    if (ch === '(') {
      depth++;
    } else if (ch === ')') {
      depth = Math.max(0, depth - 1);
    } else if (ch === separator && depth === 0) {
      const part = input.slice(start, i).trim();
      if (part) {
        parts.push(part);
      }
      start = i + 1;
    }
    i++;
  }
  const part = input.slice(start).trim();
  if (part) {
    parts.push(part);
  }
  return parts;
}

export type ParsedIdentifier = {
  display: string;
  end: number;
};

export function readQualifiedIdentifier(
  input: string,
  start = 0,
): ParsedIdentifier | undefined {
  const parts: string[] = [];
  let pos = skipWhitespace(input, start);
  for (;;) {
    const part = readIdentifier(input, pos);
    if (!part) {
      break;
    }
    parts.push(part.display);
    pos = skipWhitespace(input, part.end);
    if (input[pos] !== '.') {
      break;
    }
    pos = skipWhitespace(input, pos + 1);
  }
  if (!parts.length) {
    return undefined;
  }
  return {display: parts.join('.'), end: pos};
}

export function readIdentifier(
  input: string,
  start = 0,
): ParsedIdentifier | undefined {
  let pos = skipWhitespace(input, start);
  if (input[pos] === '"') {
    let display = '';
    pos++;
    while (pos < input.length) {
      if (input[pos] === '"') {
        if (input[pos + 1] === '"') {
          display += '"';
          pos += 2;
          continue;
        }
        return {display, end: pos + 1};
      }
      display += input[pos];
      pos++;
    }
    return undefined;
  }

  const match = /^[A-Za-z_][A-Za-z0-9_$]*/.exec(input.slice(pos));
  if (!match) {
    return undefined;
  }
  return {display: match[0], end: pos + match[0].length};
}

export function skipWhitespace(input: string, start: number): number {
  let pos = start;
  while (/\s/.test(input[pos] ?? '')) {
    pos++;
  }
  return pos;
}

export function findTopLevelKeyword(
  input: string,
  keyword: string,
  start = 0,
): number {
  const normalizedKeyword = keyword.toLowerCase().replace(/\s+/g, ' ');
  let depth = 0;
  let i = start;
  while (i < input.length) {
    const skipped = skipQuotedOrComment(input, i);
    if (skipped !== undefined) {
      i = skipped;
      continue;
    }
    const ch = input[i];
    if (ch === '(') {
      depth++;
    } else if (ch === ')') {
      depth = Math.max(0, depth - 1);
    } else if (depth === 0 && keywordMatchesAt(input, i, normalizedKeyword)) {
      return i;
    }
    i++;
  }
  return -1;
}

export function findFirstTopLevelKeyword(
  input: string,
  keywords: readonly string[],
  start = 0,
): {keyword: string; index: number} | undefined {
  let best: {keyword: string; index: number} | undefined;
  for (const keyword of keywords) {
    const index = findTopLevelKeyword(input, keyword, start);
    if (index !== -1 && (!best || index < best.index)) {
      best = {keyword, index};
    }
  }
  return best;
}

function keywordMatchesAt(
  input: string,
  index: number,
  normalizedKeyword: string,
): boolean {
  const before = input[index - 1];
  if (before && /[A-Za-z0-9_$]/.test(before)) {
    return false;
  }
  let pos = index;
  for (const part of normalizedKeyword.split(' ')) {
    if (part !== normalizedKeyword.split(' ')[0]) {
      pos = skipWhitespace(input, pos);
    }
    if (input.slice(pos, pos + part.length).toLowerCase() !== part) {
      return false;
    }
    pos += part.length;
  }
  const after = input[pos];
  return !after || !/[A-Za-z0-9_$]/.test(after);
}

function skipQuotedOrComment(input: string, start: number): number | undefined {
  const ch = input[start];
  const next = input[start + 1];
  if (ch === '-' && next === '-') {
    const end = input.indexOf('\n', start + 2);
    return end === -1 ? input.length : end;
  }
  if (ch === '/' && next === '*') {
    const end = input.indexOf('*/', start + 2);
    return end === -1 ? input.length : end + 2;
  }
  if (ch === "'") {
    return skipSingleQuoted(input, start);
  }
  if (ch === '"') {
    return skipDoubleQuoted(input, start);
  }
  if (ch === '$') {
    const delimiter = readDollarDelimiter(input, start);
    if (delimiter) {
      const end = input.indexOf(delimiter, start + delimiter.length);
      return end === -1 ? input.length : end + delimiter.length;
    }
  }
  return undefined;
}

function skipSingleQuoted(input: string, start: number): number {
  let pos = start + 1;
  while (pos < input.length) {
    if (input[pos] === '\\') {
      pos += 2;
      continue;
    }
    if (input[pos] === "'") {
      if (input[pos + 1] === "'") {
        pos += 2;
        continue;
      }
      return pos + 1;
    }
    pos++;
  }
  return input.length;
}

function skipDoubleQuoted(input: string, start: number): number {
  let pos = start + 1;
  while (pos < input.length) {
    if (input[pos] === '"') {
      if (input[pos + 1] === '"') {
        pos += 2;
        continue;
      }
      return pos + 1;
    }
    pos++;
  }
  return input.length;
}

function readDollarDelimiter(input: string, start: number): string | undefined {
  const end = input.indexOf('$', start + 1);
  if (end === -1) {
    return undefined;
  }
  const tag = input.slice(start + 1, end);
  if (tag !== '' && !/^[A-Za-z_][A-Za-z0-9_]*$/.test(tag)) {
    return undefined;
  }
  return input.slice(start, end + 1);
}

function countNewlines(input: string, start: number, end: number): number {
  let count = 0;
  for (let i = start; i < end; i++) {
    if (input[i] === '\n') {
      count++;
    }
  }
  return count;
}
