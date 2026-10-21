/**
 * A small seeded PRNG (mulberry32) so generated vectors are reproducible.
 */
export class Random {
  #state: number;

  constructor(seed: number) {
    this.#state = seed >>> 0;
  }

  /** Uniform in [0, 1). */
  next(): number {
    let t = (this.#state = (this.#state + 0x6d2b79f5) >>> 0);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  int(maxExclusive: number): number {
    return Math.floor(this.next() * maxExclusive);
  }

  pick<T>(items: readonly T[]): T {
    return items[this.int(items.length)];
  }

  bool(p = 0.5): boolean {
    return this.next() < p;
  }

  /** A random float64 drawn from its bit pattern, finite only. */
  float64(): number {
    const view = new DataView(new ArrayBuffer(8));
    for (;;) {
      view.setUint32(0, this.int(2 ** 32));
      view.setUint32(4, this.int(2 ** 32));
      const n = view.getFloat64(0);
      if (Number.isFinite(n)) {
        return n;
      }
    }
  }
}

/** Strings that exercise UTF-8 vs UTF-16 ordering and escaping. */
export const interestingStrings: readonly string[] = [
  '',
  'a',
  'A',
  'abc',
  'ab',
  'b',
  'Z',
  'z',
  ' ',
  '0',
  '9',
  '\u0000',
  '\u001f',
  '"',
  '\\',
  '\n',
  '\u00e9', // é precomposed
  'e\u0301', // é decomposed
  '\u00df', // ß
  '\u0130', // İ
  '\u0131', // ı
  '\ufb00', // ﬀ
  '\u00a0', // no-break space
  '\ue000', // private use, above the surrogates in UTF-16
  '\ufffd',
  '\uffff',
  '\u{1f600}', // 😀, a surrogate pair
  '\u{1f600}a',
  'a\u{1f600}',
  '\u{1d49c}', // 𝒜
  '\u4e2d\u6587', // 中文
  '\u03a9', // Ω
  '\u03c9', // ω
  '\u{10ffff}',
];

export function randomString(r: Random, maxLength = 8): string {
  const alphabet = [
    'a',
    'b',
    'A',
    'B',
    '_',
    '%',
    '\\',
    ' ',
    '\n',
    '\u00e9',
    '\u00df',
    '\u0130',
    '\ue000',
    '\u{1f600}',
    '\u{1d49c}',
    '\u4e2d',
    '"',
  ];
  const n = r.int(maxLength + 1);
  let s = '';
  for (let i = 0; i < n; i++) {
    s += r.pick(alphabet);
  }
  return s;
}

/** The big-endian hex bit pattern of a float64, exact for -0, NaN, etc. */
export function float64Bits(n: number): string {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, n);
  return (
    view.getUint32(0).toString(16).padStart(8, '0') +
    view.getUint32(4).toString(16).padStart(8, '0')
  );
}
