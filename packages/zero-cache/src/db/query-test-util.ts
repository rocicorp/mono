export function stripCommentsAndWhitespace(query: string = '') {
  return query
    .trim()
    .replaceAll(/--.*\n/g, '')
    .replaceAll(/\s+/g, ' ');
}
