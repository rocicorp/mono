export function like(strings: TemplateStringsArray, ...values: unknown[]) {
  strings.map(validateLiteral);

  let result = strings[0];
  for (let i = 0; i < values.length; i++) {
    result += escapeLike(String(values[i])) + strings[i + 1];
  }

  return result;
}

function validateLiteral(value: string) {
  const match = value.match(/[^%_]/);
  if (match) {
    throw new Error(`Invalid character '${match[0]}' in LIKE pattern`);
  }
}

function escapeLike(value: string) {
  return value.replace(/[%_]/g, '\\$&');
}
