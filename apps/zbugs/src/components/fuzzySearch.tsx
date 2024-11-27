export function fuzzySearch<T>(
  searchQuery: string,
  items: readonly T[],
  itemToString: (item: T) => string,
): T[] {
  // case insensitive
  // character first in string is higher priority
  // character first in word is higher priority
  const q = searchQuery.toLowerCase();

  function getRank(q: string, text: string): number {
    text = text.toLowerCase();
    let rank = 0;
    let i = 0;
    let j = 0;
    // 10 for starts with, 5 for whitespace, 1 for other
    let multiplier = 10;

    while (i < q.length && j < text.length) {
      if (q[i] === text[j]) {
        rank += 1 * multiplier;
        i++;
        multiplier = 1.1;
      } else {
        if (isWhitespace(text[j])) {
          multiplier = 5;
        }
        multiplier = 1;
      }
      j++;
    }

    if (i < q.length) {
      return 0;
    }

    return rank;
  }

  function isWhitespace(c: string): boolean {
    return /\s/.test(c);
  }

  const ranked = items
    .map(item => ({
      item,
      rank: getRank(q, itemToString(item)),
    }))
    .filter(r => r.rank > 0);
  ranked.sort((a, b) => {
    if (b.rank === a.rank) {
      return itemToString(a.item).localeCompare(itemToString(b.item));
    }
    return b.rank - a.rank;
  });
  return ranked.map(r => r.item);
}
