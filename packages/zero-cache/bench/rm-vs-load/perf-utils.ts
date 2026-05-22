import {mkdir, writeFile} from 'node:fs/promises';
import {dirname} from 'node:path';

export function percentile(values: readonly number[], p: number): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = values.toSorted((a, b) => a - b);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
  );
  return sorted[index] ?? 0;
}

export function sum(values: readonly number[]): number {
  let total = 0;
  for (const value of values) {
    total += value;
  }
  return total;
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ['KiB', 'MiB', 'GiB'] as const;
  let value = bytes / 1024;
  let unit: (typeof units)[number] = units[0];
  for (let i = 1; i < units.length && value >= 1024; i++) {
    value /= 1024;
    unit = units[i];
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`;
}

export function formatRate(value: number): string {
  return value.toLocaleString('en-US', {maximumFractionDigits: 1});
}

export async function writeJsonSummary(
  summary: unknown,
  outputPath: string | undefined,
): Promise<void> {
  if (outputPath !== undefined) {
    await mkdir(dirname(outputPath), {recursive: true});
    await writeFile(outputPath, JSON.stringify(summary, null, 2) + '\n');
  }
}
