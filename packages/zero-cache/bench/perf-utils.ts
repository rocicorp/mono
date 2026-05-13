import {writeFile} from 'node:fs/promises';

export function argValue(name: string): string | undefined {
  const long = `--${name}`;
  const withEquals = `${long}=`;
  for (let i = 2; i < process.argv.length; i++) {
    const arg = process.argv[i];
    if (arg === long) {
      return process.argv[i + 1];
    }
    if (arg?.startsWith(withEquals)) {
      return arg.slice(withEquals.length);
    }
  }
  return undefined;
}

export function envString(
  name: string,
  fallback: string | undefined = undefined,
): string | undefined {
  const value = process.env[name];
  return value === undefined || value === '' ? fallback : value;
}

export function envInt(name: string, fallback: number): number {
  const value = envString(name);
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid integer ${name}=${value}`);
  }
  return parsed;
}

export function envNumber(name: string, fallback: number): number {
  const value = envString(name);
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid number ${name}=${value}`);
  }
  return parsed;
}

export function envFlag(name: string): boolean {
  const value = envString(name);
  return value === '1' || value === 'true' || value === 'yes';
}

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
    await writeFile(outputPath, JSON.stringify(summary, null, 2) + '\n');
  }
}
