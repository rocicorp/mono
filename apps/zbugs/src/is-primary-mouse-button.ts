export function isPrimaryMouseButton(e: MouseEvent) {
  return !(e.ctrlKey || e.metaKey || e.altKey || e.shiftKey || e.button !== 0);
}
