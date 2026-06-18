import {isAbsolute, resolve, relative} from 'node:path';
import {
  analyzeSqlPaths,
  type AnalysisResult,
  type Severity,
} from './analyzer.ts';

type CliOptions = {
  json: boolean;
  failOn?: Severity | undefined;
  zeroVersion?: string | undefined;
  paths: string[];
};

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.paths.length === 0) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  const result = await analyzeSqlPaths(options.paths.map(resolveInputPath), {
    zeroVersion: options.zeroVersion,
  });
  if (options.json) {
    process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  } else {
    process.stdout.write(formatMarkdown(result) + '\n');
  }

  if (options.failOn && shouldFail(result, options.failOn)) {
    process.exitCode = 1;
  }
}

function parseArgs(args: readonly string[]): CliOptions {
  const options: CliOptions = {json: false, paths: []};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--') {
      continue;
    }
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--fail-on') {
      const severity = args[++i];
      if (!isSeverity(severity)) {
        throw new Error('--fail-on must be one of: info, warning, error');
      }
      options.failOn = severity;
      continue;
    }
    if (arg === '--zero-version') {
      const version = args[++i];
      if (!version) {
        throw new Error('--zero-version requires a version');
      }
      options.zeroVersion = version;
      continue;
    }
    if (arg.startsWith('--zero-version=')) {
      options.zeroVersion = arg.slice('--zero-version='.length);
      continue;
    }
    if (arg.startsWith('--fail-on=')) {
      const severity = arg.slice('--fail-on='.length);
      if (!isSeverity(severity)) {
        throw new Error('--fail-on must be one of: info, warning, error');
      }
      options.failOn = severity;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      printUsage();
      process.exit(0);
    }
    options.paths.push(arg);
  }
  return options;
}

function resolveInputPath(path: string): string {
  return isAbsolute(path)
    ? path
    : resolve(process.env.INIT_CWD ?? process.cwd(), path);
}

function isSeverity(value: string | undefined): value is Severity {
  return value === 'info' || value === 'warning' || value === 'error';
}

function shouldFail(result: AnalysisResult, failOn: Severity): boolean {
  const ranks: Record<Severity, number> = {info: 0, warning: 1, error: 2};
  return result.findings.some(
    finding => ranks[finding.severity] >= ranks[failOn],
  );
}

function formatMarkdown(result: AnalysisResult): string {
  const lines: string[] = [];
  lines.push('# Zero Migration Impact');
  lines.push('');
  lines.push(`Files: ${result.files.length}`);
  lines.push(`Statements analyzed: ${result.statementsAnalyzed}`);
  lines.push(`Zero version: ${result.zeroVersion.label}`);
  lines.push('');
  lines.push('## Summary');
  lines.push('');
  lines.push(`- Safety: ${result.summary.safety}`);
  lines.push(`- Backfill: ${result.summary.backfill}`);
  lines.push(
    `- SchemaVersionNotSupported: ${result.summary.schemaVersionNotSupported}`,
  );
  lines.push(`- Replication lag risk: ${result.summary.replicationLag}`);

  if (!result.findings.length) {
    lines.push('');
    lines.push('No Zero-specific risks found in the SQL patterns analyzed.');
    return lines.join('\n');
  }

  lines.push('');
  lines.push('## Findings');
  for (const [index, finding] of result.findings.entries()) {
    lines.push('');
    lines.push(`${index + 1}. ${finding.title}`);
    lines.push(`   - Severity: ${finding.severity}`);
    lines.push(`   - Location: ${formatLocation(finding.location)}`);
    lines.push(
      `   - Impact: backfill=${finding.impact.backfill}, SchemaVersionNotSupported=${finding.impact.schemaVersionNotSupported}, replicationLag=${finding.impact.replicationLag}`,
    );
    lines.push(`   - Details: ${finding.details}`);
    lines.push(`   - Statement: \`${finding.statement}\``);
    lines.push(`   - Remediation: ${finding.remediations.join(' ')}`);
  }
  return lines.join('\n');
}

function formatLocation(location: {
  file?: string | undefined;
  startLine: number;
  endLine: number;
}) {
  const cwd = process.env.INIT_CWD ?? process.cwd();
  const file = location.file ? relative(cwd, location.file) : '<sql>';
  return location.startLine === location.endLine
    ? `${file}:${location.startLine}`
    : `${file}:${location.startLine}-${location.endLine}`;
}

function printUsage() {
  process.stdout.write(`Usage:
  pnpm --filter zero-migration-impact run analyze -- [options] <file-or-dir>...

Options:
  --json                  Print machine-readable JSON.
  --zero-version <ver>    Analyze against a Zero version, e.g. 0.25.0, 0.26.0, or current.
  --fail-on <severity>    Exit non-zero when a finding has severity info, warning, or error.
  -h, --help              Show this help.
`);
}

main().catch(err => {
  process.stderr.write(
    (err instanceof Error ? err.message : String(err)) + '\n',
  );
  process.exitCode = 1;
});
