const isMac = !!navigator.platform?.toLowerCase().includes('mac');

export function isCtrlEnter(e: React.KeyboardEvent): boolean {
  return e.key === 'Enter' && (isMac ? e.metaKey : e.ctrlKey);
}
