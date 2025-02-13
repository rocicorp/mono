export function elide(val: string, maxBytes: number) {
  const encoder = new TextEncoder();
  if (encoder.encode(val).length <= maxBytes) {
    return val;
  }
  val = val.substring(0, maxBytes - 3);
  while (encoder.encode(val + '...').length > maxBytes) {
    val = val.substring(0, val.length - 1);
  }
  return val + '...';
}
