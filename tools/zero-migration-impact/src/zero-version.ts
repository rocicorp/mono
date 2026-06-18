export type ZeroVersionProfile = {
  label: string;
  supportsAddColumnDefaults: boolean;
  autoBackfillsUnsupportedDefaults: boolean;
};

type ParsedVersion = {
  major: number;
  minor: number;
  patch: number;
  prerelease: string[];
};

const ZERO_PREFIX_RE = /^zero\//;
const VERSION_PREFIX_RE = /^v/;
const VERSION_RE = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$/;
const NUMERIC_PRERELEASE_RE = /^\d+$/;

const ADD_COLUMN_DEFAULT_SUPPORT_VERSION = parseZeroVersion('0.0.202410040736');

// feat: auto-backfill table/column data for schema changes (#5570).
const AUTO_BACKFILL_SCHEMA_CHANGES_VERSION =
  parseZeroVersion('0.26.0-canary.7');

export function profileForZeroVersion(
  version: string | undefined,
): ZeroVersionProfile {
  if (!version || version === 'current' || version === 'latest') {
    return {
      label: version ?? 'current',
      supportsAddColumnDefaults: true,
      autoBackfillsUnsupportedDefaults: true,
    };
  }

  const parsed = parseZeroVersion(version);
  return {
    label: version,
    supportsAddColumnDefaults:
      compareVersions(parsed, ADD_COLUMN_DEFAULT_SUPPORT_VERSION) >= 0,
    autoBackfillsUnsupportedDefaults:
      compareVersions(parsed, AUTO_BACKFILL_SCHEMA_CHANGES_VERSION) >= 0,
  };
}

function parseZeroVersion(version: string): ParsedVersion {
  const normalized = version
    .trim()
    .replace(ZERO_PREFIX_RE, '')
    .replace(VERSION_PREFIX_RE, '')
    .split('+')[0];
  const match = VERSION_RE.exec(normalized);
  if (!match) {
    throw new Error(
      `Invalid Zero version "${version}". Expected a version like 0.26.0, 0.26.0-canary.7, or current.`,
    );
  }
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease: match[4]?.split('.') ?? [],
  };
}

function compareVersions(left: ParsedVersion, right: ParsedVersion): number {
  for (const key of ['major', 'minor', 'patch'] as const) {
    const diff = left[key] - right[key];
    if (diff !== 0) {
      return diff;
    }
  }
  return comparePrerelease(left.prerelease, right.prerelease);
}

function comparePrerelease(left: readonly string[], right: readonly string[]) {
  if (!left.length && !right.length) {
    return 0;
  }
  if (!left.length) {
    return 1;
  }
  if (!right.length) {
    return -1;
  }
  const length = Math.max(left.length, right.length);
  for (let i = 0; i < length; i++) {
    const leftPart = left[i];
    const rightPart = right[i];
    if (leftPart === undefined) {
      return -1;
    }
    if (rightPart === undefined) {
      return 1;
    }
    const leftNumber = numericPrereleasePart(leftPart);
    const rightNumber = numericPrereleasePart(rightPart);
    if (leftNumber !== undefined && rightNumber !== undefined) {
      const diff = leftNumber - rightNumber;
      if (diff !== 0) {
        return diff;
      }
      continue;
    }
    if (leftNumber !== undefined) {
      return -1;
    }
    if (rightNumber !== undefined) {
      return 1;
    }
    const diff = leftPart.localeCompare(rightPart);
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
}

function numericPrereleasePart(part: string): number | undefined {
  return NUMERIC_PRERELEASE_RE.test(part) ? Number(part) : undefined;
}
