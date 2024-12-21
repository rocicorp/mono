export function isPrimaryMouseButton(e: React.MouseEvent) {
  return !(e.ctrlKey || e.metaKey || e.altKey || e.shiftKey || e.button !== 0);
}
