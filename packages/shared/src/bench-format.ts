// oxlint-disable no-console

// Formatting utilities adapted from mitata's internal `$` object and
// the `mitata` format handler in main.mjs.
// https://github.com/evanwashere/mitata/blob/main/src/main.mjs
//
// Copyright 2022 evanwashere
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

function clamp(m: number, v: number, x: number) {
  return v < m ? m : v > x ? x : v;
}

function arrMax(arr: number[]) {
  return arr.reduce((x, v) => Math.max(x, v), -Infinity);
}

function fmtStr(s: string, len: number) {
  if (len >= s.length) return s;
  return `${s.slice(0, len - 2)}..`;
}

function fmtTime(ns: number) {
  if (ns < 1) return `${(ns * 1e3).toFixed(2)} ps`;
  if (ns < 1e3) return `${ns.toFixed(2)} ns`;
  ns /= 1000;
  if (ns < 1e3) return `${ns.toFixed(2)} µs`;
  ns /= 1000;
  if (ns < 1e3) return `${ns.toFixed(2)} ms`;
  ns /= 1000;
  if (ns < 1e3) return `${ns.toFixed(2)} s`;
  ns /= 60;
  if (ns < 1e3) return `${ns.toFixed(2)} m`;
  ns /= 60;
  return `${ns.toFixed(2)} h`;
}

function fmtBytes(b: number) {
  if (Number.isNaN(b)) return 'NaN';
  if (b < 1e3) return `${b.toFixed(2)}  b`;
  b /= 1024;
  if (b < 1e3) return `${b.toFixed(2)} kb`;
  b /= 1024;
  if (b < 1e3) return `${b.toFixed(2)} mb`;
  b /= 1024;
  if (b < 1e3) return `${b.toFixed(2)} gb`;
  b /= 1024;
  return `${b.toFixed(2)} tb`;
}

const ansi = {
  bold: '\x1b[1m',
  reset: '\x1b[0m',
  cyan: '\x1b[36m',
  gray: '\x1b[90m',
  yellow: '\x1b[33m',
  magenta: '\x1b[35m',
};

const histogramSymbols = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'] as const;

export interface Stats {
  min: number;
  max: number;
  avg: number;
  p75: number;
  p99: number;
  samples: number[];
  heap?: {avg: number; min: number; max: number} | undefined;
}

interface Bins {
  min: number;
  max: number;
  step: number;
  bins: number[];
  peak: number;
  avg: number;
}

function histogramBins(stats: Stats, size: number, percentile: number): Bins {
  const offset = (percentile * (stats.samples.length - 1)) | 0;
  let min = stats.min;
  const max = stats.samples[offset] || stats.max || 1;
  const bins = Array.from<number>({length: size}).fill(0);
  const step = (max - min) / (size - 1);

  if (0 === step) {
    min = 0;
    bins[clamp(0, Math.round((stats.avg - min) / step), size - 1)] = 1;
  } else {
    for (let o = 0; o <= offset; o++) {
      bins[Math.round((stats.samples[o] - min) / step)]++;
    }
  }

  return {
    min,
    max,
    step,
    bins,
    peak: arrMax(bins),
    avg: clamp(0, Math.round((stats.avg - min) / step), size - 1),
  };
}

function histogramAscii(
  _bins: Bins,
  height: number,
  colors: boolean,
): string[] {
  const symbols = histogramSymbols;
  const canvas = Array.from<string>({length: height});
  const {avg, peak, bins} = _bins;
  const scale = (height * symbols.length - 1) / peak;

  for (let y = 0; y < height; y++) {
    let l = '';

    if (0 !== avg) {
      if (colors) l += ansi.cyan;
      for (let o = 0; o < avg; o++) {
        const b = bins[o];
        if (y === 0) {
          l += symbols[clamp(0, Math.round(b * scale), symbols.length - 1)];
        } else {
          const lo = y * symbols.length;
          const hi = (y + 1) * symbols.length;
          const offset = Math.round(b * scale) | 0;
          if (lo >= offset) l += ' ';
          else if (hi <= offset) l += symbols[symbols.length - 1];
          else l += symbols[clamp(lo, offset, hi) % symbols.length];
        }
      }
      if (colors) l += ansi.reset;
    }

    {
      if (colors) l += ansi.yellow;
      const b = bins[avg];
      if (y === 0) {
        l += symbols[clamp(0, Math.round(b * scale), symbols.length - 1)];
      } else {
        const lo = y * symbols.length;
        const hi = (y + 1) * symbols.length;
        const offset = Math.round(b * scale) | 0;
        if (lo >= offset) l += ' ';
        else if (hi <= offset) l += symbols[symbols.length - 1];
        else l += symbols[clamp(lo, offset, hi) % symbols.length];
      }
      if (colors) l += ansi.reset;
    }

    if (avg !== bins.length - 1) {
      if (colors) l += ansi.magenta;
      for (let o = 1 + avg; o < bins.length; o++) {
        const b = bins[o];
        if (y === 0) {
          l += symbols[clamp(0, Math.round(b * scale), symbols.length - 1)];
        } else {
          const lo = y * symbols.length;
          const hi = (y + 1) * symbols.length;
          const offset = Math.round(b * scale) | 0;
          if (lo >= offset) l += ' ';
          else if (hi <= offset) l += symbols[symbols.length - 1];
          else l += symbols[clamp(lo, offset, hi) % symbols.length];
        }
      }
      if (colors) l += ansi.reset;
    }

    canvas[y] = l;
  }

  return canvas.reverse();
}

export function printBenchHeader(nameWidth: number, colors: boolean) {
  const print = console.log;
  const nameCol = 'benchmark'.padEnd(nameWidth);
  const header = nameCol + ' avg (min … max) p75 / p99    (min … top 1%)';
  const sep = '-'.repeat(nameWidth + 13) + ' ' + '-'.repeat(31);
  if (colors) {
    print(ansi.bold + header + ansi.reset);
    print(ansi.gray + sep + ansi.reset);
  } else {
    print(header);
    print(sep);
  }
}

export function printBenchResult(
  name: string,
  stats: Stats,
  nameWidth: number,
  colors: boolean,
) {
  const print = console.log;
  const avg = fmtTime(stats.avg).padStart(9);
  const paddedName = fmtStr(name, nameWidth).padEnd(nameWidth);

  let l = paddedName + ' ';
  const p75 = fmtTime(stats.p75).padStart(9);
  const histBins = histogramBins(stats, 21, 0.99);
  const histHeight = stats.heap ? 3 : 2;
  const histogram = histogramAscii(histBins, histHeight, colors);

  if (!colors) l += avg + '/iter' + ' ' + p75 + ' ' + histogram[0];
  else {
    l +=
      ansi.bold +
      ansi.yellow +
      avg +
      ansi.reset +
      ansi.bold +
      '/iter' +
      ansi.reset +
      ' ' +
      ansi.gray +
      p75 +
      ansi.reset +
      ' ' +
      histogram[0];
  }
  print(l);

  l = '';
  const min = fmtTime(stats.min);
  const max = fmtTime(stats.max);
  const p99 = fmtTime(stats.p99).padStart(9);
  const diff = 2 * 9 - (min.length + max.length);

  l += ' '.repeat(diff + nameWidth - 8);
  if (!colors) l += '(' + min + ' … ' + max + ')';
  else {
    l +=
      ansi.gray +
      '(' +
      ansi.reset +
      ansi.cyan +
      min +
      ansi.reset +
      ansi.gray +
      ' … ' +
      ansi.reset +
      ansi.magenta +
      max +
      ansi.reset +
      ansi.gray +
      ')' +
      ansi.reset;
  }
  l += ' ';
  if (!colors) l += p99 + ' ' + histogram[1];
  else l += ansi.gray + p99 + ansi.reset + ' ' + histogram[1];
  print(l);

  if (stats.heap) {
    l = ' '.repeat(nameWidth - 8);
    const hm = fmtBytes(stats.heap.min).padStart(9);
    const hx = fmtBytes(stats.heap.max).padStart(9);
    const ha = fmtBytes(stats.heap.avg).padStart(9);

    if (!colors) l += '(' + hm + ' … ' + hx + ') ' + ha + ' ' + histogram[2];
    else {
      l +=
        ansi.gray +
        '(' +
        ansi.reset +
        ansi.yellow +
        hm +
        ansi.reset +
        ansi.gray +
        ' … ' +
        ansi.reset +
        ansi.yellow +
        hx +
        ansi.reset +
        ansi.gray +
        ') ' +
        ansi.reset +
        ansi.yellow +
        ha +
        ansi.reset +
        ' ' +
        histogram[2];
    }
    print(l);
  }
}
