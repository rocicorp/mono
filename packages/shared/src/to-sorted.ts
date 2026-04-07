// toSorted was added in ES2023
export function toSorted<T>(arr: T[], compare?: (a: T, b: T) => number) {
  return [...arr].sort(compare);
}
