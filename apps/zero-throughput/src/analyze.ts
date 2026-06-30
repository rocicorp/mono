import {runAnalyzeCLI} from '../../../packages/zero/src/analyze.ts';
import {createBuilder} from '../../../packages/zql/src/query/create-builder.ts';
import type {BenchmarkProfile} from './config.ts';
import {
  buildProfileQuery,
  findProfileQuery,
  PROFILE_QUERY_NAMES,
} from './profile-queries.ts';
import {schema} from './schema.ts';

const DEFAULT_PROFILE = 'relational' satisfies BenchmarkProfile;
const DEFAULT_QUERY_INDEX = 0;
const DEFAULT_ROWS_PER_QUERY = 100;
const PROFILES = new Set<BenchmarkProfile>([
  'feed-append',
  'email',
  'forum',
  'relational',
]);

type AnalyzeConfig = {
  readonly passThroughArgs: readonly string[];
  readonly profile: BenchmarkProfile | undefined;
  readonly profileQueryName: string | undefined;
  readonly queryIndex: number;
  readonly rowsPerQuery: number;
  readonly printAst: boolean;
  readonly listProfileQueries: boolean;
  readonly help: boolean;
  readonly usesProfileOptions: boolean;
};

const config = parseArgs(process.argv.slice(2));

if (config.help) {
  printUsage();
} else if (config.listProfileQueries) {
  printProfileQueries();
} else if (hasAnalyzeQuerySelector(config.passThroughArgs)) {
  if (config.usesProfileOptions || config.printAst) {
    throw new Error(
      'Use either profile query options or --ast/--query/--query-name, not both.',
    );
  }
  await runAnalyzeCLI({schema, argv: config.passThroughArgs});
} else {
  const {profile, queryIndex} = resolveProfileQuery(config);
  const {name, query} = buildProfileQuery(
    createBuilder(schema),
    profile,
    queryIndex,
    config.rowsPerQuery,
  );
  const ast = queryAST(query);
  if (config.printAst) {
    stdout(`${JSON.stringify(ast)}\n`);
  } else {
    stderr(`Analyzing ${name} with rowsPerQuery=${config.rowsPerQuery}\n`);
    await runAnalyzeCLI({
      schema,
      argv: [...config.passThroughArgs, `--ast=${JSON.stringify(ast)}`],
    });
  }
}

function parseArgs(argv: readonly string[]): AnalyzeConfig {
  const passThroughArgs: string[] = [];
  let profile: BenchmarkProfile | undefined;
  let profileQueryName: string | undefined;
  let queryIndex = DEFAULT_QUERY_INDEX;
  let rowsPerQuery = DEFAULT_ROWS_PER_QUERY;
  let printAst = false;
  let listProfileQueries = false;
  let help = false;
  let usesProfileOptions = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const option = parseOption(arg);
    switch (option.name) {
      case '--help':
      case '-h':
        help = true;
        break;

      case '--profile':
        profile = parseProfile(readOptionValue(argv, option, i));
        i += option.value === undefined ? 1 : 0;
        usesProfileOptions = true;
        break;

      case '--profile-query':
      case '--profileQuery':
        profileQueryName = readOptionValue(argv, option, i);
        i += option.value === undefined ? 1 : 0;
        usesProfileOptions = true;
        break;

      case '--query-index':
      case '--queryIndex':
        queryIndex = parseNonNegativeInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        usesProfileOptions = true;
        break;

      case '--rows-per-query':
      case '--rowsPerQuery':
        rowsPerQuery = parsePositiveInteger(
          option.name,
          readOptionValue(argv, option, i),
        );
        i += option.value === undefined ? 1 : 0;
        usesProfileOptions = true;
        break;

      case '--print-ast':
      case '--printAst':
        printAst = parseBooleanOption(option.value, true);
        usesProfileOptions = true;
        break;

      case '--list-profile-queries':
      case '--listProfileQueries':
        listProfileQueries = parseBooleanOption(option.value, true);
        usesProfileOptions = true;
        break;

      default:
        passThroughArgs.push(arg);
    }
  }

  return {
    passThroughArgs,
    profile,
    profileQueryName,
    queryIndex,
    rowsPerQuery,
    printAst,
    listProfileQueries,
    help,
    usesProfileOptions,
  };
}

function queryAST(query: unknown): unknown {
  if (query === null || typeof query !== 'object' || !('ast' in query)) {
    throw new Error('Profile query did not expose an AST');
  }
  return (query as {readonly ast: unknown}).ast;
}

function parseOption(arg: string): {
  readonly name: string;
  readonly value: string | undefined;
} {
  const equals = arg.indexOf('=');
  if (equals === -1) {
    return {name: arg, value: undefined};
  }
  return {name: arg.slice(0, equals), value: arg.slice(equals + 1)};
}

function readOptionValue(
  argv: readonly string[],
  option: {readonly name: string; readonly value: string | undefined},
  index: number,
): string {
  if (option.value !== undefined) {
    return option.value;
  }
  const value = argv[index + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${option.name} requires a value`);
  }
  return value;
}

function parseProfile(value: string): BenchmarkProfile {
  if (PROFILES.has(value as BenchmarkProfile)) {
    return value as BenchmarkProfile;
  }
  throw new Error(
    `Invalid profile "${value}". Expected one of: ${[...PROFILES].join(', ')}`,
  );
}

function parsePositiveInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function parseBooleanOption(
  value: string | undefined,
  defaultValue: boolean,
): boolean {
  if (value === undefined) {
    return defaultValue;
  }
  switch (value) {
    case 'true':
      return true;
    case 'false':
      return false;
    default:
      throw new Error(`Expected boolean value, got "${value}"`);
  }
}

function hasAnalyzeQuerySelector(argv: readonly string[]): boolean {
  return argv.some(
    arg =>
      arg === '--ast' ||
      arg.startsWith('--ast=') ||
      arg === '--query' ||
      arg.startsWith('--query=') ||
      arg === '--query-name' ||
      arg.startsWith('--query-name=') ||
      arg === '--queryName' ||
      arg.startsWith('--queryName='),
  );
}

function resolveProfileQuery(config: AnalyzeConfig): {
  readonly profile: BenchmarkProfile;
  readonly queryIndex: number;
} {
  if (config.profileQueryName !== undefined) {
    const found = findProfileQuery(config.profileQueryName);
    if (found === undefined) {
      throw new Error(`Unknown profile query "${config.profileQueryName}"`);
    }
    if (config.profile !== undefined && found.profile !== config.profile) {
      throw new Error(
        `Profile query ${config.profileQueryName} belongs to ${found.profile}, not ${config.profile}`,
      );
    }
    return found;
  }
  return {
    profile: config.profile ?? DEFAULT_PROFILE,
    queryIndex: config.queryIndex,
  };
}

function printUsage(): void {
  stdout(`Usage:
  pnpm --filter zero-throughput run analyze -- --zero-cache-url=http://127.0.0.1:4848 [options]

Profile query options:
  --profile relational --query-index 2 --rows-per-query 50
  --profile-query relational:activity-list
  --print-ast
  --list-profile-queries

Any other flags are passed through to analyze-query, such as --join-plans,
--output-vended-rows, --output-synced-rows, --admin-password, or --user-id.

Defaults:
  --profile ${DEFAULT_PROFILE}
  --query-index ${DEFAULT_QUERY_INDEX}
  --rows-per-query ${DEFAULT_ROWS_PER_QUERY}
`);
  printProfileQueries();
}

function printProfileQueries(): void {
  for (const [profile, names] of Object.entries(PROFILE_QUERY_NAMES)) {
    stdout(`${profile}:\n`);
    for (const [index, name] of names.entries()) {
      stdout(`  ${index}: ${name}\n`);
    }
  }
}

function stdout(message: string): void {
  process.stdout.write(message);
}

function stderr(message: string): void {
  process.stderr.write(message);
}
