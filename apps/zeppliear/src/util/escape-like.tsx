export function escapeLike(s: string): string {
  return s.replace(/_|%/g, c => '\\' + c);
}
