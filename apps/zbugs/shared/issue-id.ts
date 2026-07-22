const nonDigitRegex = /[^\d]/;

export function getIDFromString(idStr: string) {
  const idField = nonDigitRegex.test(idStr) ? 'id' : 'shortID';
  const idValue = idField === 'shortID' ? parseInt(idStr) : idStr;
  return {idField, idValue} as const;
}
