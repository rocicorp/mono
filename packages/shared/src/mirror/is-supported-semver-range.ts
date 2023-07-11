import type {Comparator, Range} from 'semver';

export function isSupportedSemverRange(range: Range): boolean {
  if (range.set.length === 1) {
    const comparators = range.set[0];
    if (comparators.length === 1) {
      const comparator = comparators[0];
      const {operator} = comparator;
      if (operator === '<' || operator === '<=') {
        const {semver} = comparator;
        return semver.patch === 0 && (semver.major === 0 || semver.minor === 0);
      }
      return operator === '>=' || operator === '>';
    }

    if (comparators.length === 2) {
      return isComparatorOK(comparators[0]) && isComparatorOK(comparators[1]);
    }
  }

  return false;

  function isComparatorOK(comparator: Comparator): boolean {
    const {operator} = comparator;
    if (operator === '<' || operator === '<=') {
      if (comparator.semver.major === 0) {
        return true;
      }
      return comparator.semver.minor === 0 && comparator.semver.patch === 0;
    }
    return operator === '>' || operator === '>=';
  }
}
