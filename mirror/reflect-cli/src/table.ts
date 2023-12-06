type PaddingSide = 'left' | 'right';

export function padColumns(
  table: string[][],
  pad = ' ',
  side: PaddingSide = 'right',
) {
  const maxLens = table.reduce<number[]>(
    (maxLens, row) => maxLens.map((max, i) => Math.max(max, len(row[i]))),
    Array(table[0].length).fill(0) as number[],
  );
  return table.map(row =>
    row.map((val, i) => {
      const padding = pad.repeat(maxLens[i] - len(val));
      return side === 'right' ? val + padding : padding + val;
    }),
  );
}

// Strip the escape sequences used for coloring the console output when
// calculating the length of the string.
//
// Regex source: https://stackoverflow.com/a/29497680
const ANSI_ESCAPE_SEQUENCES =
  // eslint-disable-next-line no-control-regex
  /[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g;

function len(str: string): number {
  return str.replace(ANSI_ESCAPE_SEQUENCES, '').length;
}
