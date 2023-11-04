type PaddingSide = 'left' | 'right';

export function padColumns(
  table: string[][],
  pad = ' ',
  side: PaddingSide = 'right',
) {
  const maxLens = table.reduce<number[]>(
    (maxLens, row) => maxLens.map((max, i) => Math.max(max, row[i].length)),
    Array(table[0].length).fill(0) as number[],
  );
  return table.map(row =>
    row.map((val, i) => {
      const padding = pad.repeat(maxLens[i] - val.length);
      return side === 'right' ? val + padding : padding + val;
    }),
  );
}
