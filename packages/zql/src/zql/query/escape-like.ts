export function escapeLike(val: string) {
  return val.replace(/[%_]/g, '\\$&');
}
