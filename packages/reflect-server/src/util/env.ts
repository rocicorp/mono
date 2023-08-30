export function isTrueEnvValue(value: string | undefined) {
  switch ((value ?? '0').toLowerCase()) {
    case 'true':
    case '1':
      return true;
    default:
      return false;
  }
}
