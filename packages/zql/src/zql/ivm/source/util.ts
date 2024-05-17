import type {Ordering, Selector} from '../../ast/ast.js';

export function sourcesAreIdentical(
  sourceAName: string,
  sourceAOrder: Ordering,
  sourceBName: string,
  sourceBOrder: Ordering,
) {
  if (sourceAName !== sourceBName) {
    return false;
  }

  if (sourceAOrder[0].length !== sourceBOrder[0].length) {
    return false;
  }

  if (sourceAOrder[1] !== sourceBOrder[1]) {
    return false;
  }

  return sourceAOrder[0].every((col, i) =>
    selectorsAreEqual(sourceBOrder[0][i], col),
  );
}

export function selectorsAreEqual(l: Selector, r: Selector) {
  return l[0] === r[0] && l[1] === r[1];
}
