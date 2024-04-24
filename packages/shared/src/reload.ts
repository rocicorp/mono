export function reload(): void {
  if (typeof location !== 'undefined') {
    location.reload();
  }
}
