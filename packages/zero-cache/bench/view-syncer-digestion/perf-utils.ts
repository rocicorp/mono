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

export function envString(name: string, fallback?: string): string | undefined {
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
