/**
 * Returns the document object. This is wrapped in a function because Reflect
 * runs in environments that do not have a document (such as Web Workers, Deno
 * etc)
 */
export function getDocument(): Document | undefined {
  return typeof document !== 'undefined' ? document : undefined;
}
