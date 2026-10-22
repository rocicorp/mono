/**
 * `zero-swift codegen`: generates the Swift row types, schema, and query
 * builder of a Zero app from its TypeScript schema.
 *
 *   zero-swift codegen [--schema-file schema.ts] [--out Schema.swift]
 *                      [--config zero-swift.json] [--check]
 *
 * Options override the config file, which defaults to `zero-swift.json` when
 * present.
 *
 * `zero-swift drift record`: runs the app's TS queries and mutators on the
 * samples a module exports and writes what they did, for the Swift drift
 * test (see drift.ts).
 *
 *   zero-swift drift record [--samples drift-samples.ts]
 *                           [--out drift-vectors.json] [--check]
 *
 * `--check` exits with an error when the output is out of date instead of
 * writing it. The CLI is built on incur, so `--help`, `--json`, `--llms` and
 * `skills add` work as they do for other incur CLIs.
 */
import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import {relative, resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {Cli, z} from 'incur';
import type {Schema} from '../../zero-types/src/schema.ts';
import pkg from '../package.json' with {type: 'json'};
import {generate, type Config} from './generate.ts';
import {readCustomTypes} from './types.ts';

/** A mistake in how the CLI was run, reported as an error, not a crash. */
export class UsageError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

function mustExist(path: string, code: string, message: string) {
  if (!existsSync(path)) {
    throw new UsageError(code, message);
  }
}

export async function codegenFile(config: Config, cwd: string) {
  const schemaPath = resolve(cwd, config.schema ?? 'schema.ts');
  mustExist(
    schemaPath,
    'SCHEMA_NOT_FOUND',
    `Schema ${relative(cwd, schemaPath)} doesn't exist. Pass --schema-file, or set "schema" in zero-swift.json.`,
  );
  const exportName = config.export ?? 'schema';
  const module = (await import(pathToFileURL(schemaPath).href)) as Record<
    string,
    unknown
  >;
  const schema = module[exportName] as Schema | undefined;
  if (!schema || typeof schema !== 'object' || !('tables' in schema)) {
    throw new UsageError(
      'SCHEMA_NOT_EXPORTED',
      `${relative(cwd, schemaPath)} does not export a schema as "${exportName}". Pass --export, or set "export" in zero-swift.json.`,
    );
  }
  const customTypes = readCustomTypes(schemaPath, exportName);
  return generate(
    schema,
    customTypes,
    config,
    relative(cwd, schemaPath).replaceAll('\\', '/'),
  );
}

type Fail = (options: {
  code: string;
  message: string;
  exitCode?: number | undefined;
}) => never;

/** Runs `body`, turning a `UsageError` into the command's error result. */
async function reporting<T>(fail: Fail, body: () => Promise<T>): Promise<T> {
  try {
    return await body();
  } catch (e) {
    if (e instanceof UsageError) {
      return fail({code: e.code, message: e.message});
    }
    throw e;
  }
}

/**
 * Writes `contents` and returns what it did, or with `check`, fails when the
 * file differs.
 */
function writeOutput(
  fail: Fail,
  output: string,
  contents: string,
  check: boolean,
  cwd: string,
) {
  const path = relative(cwd, output);
  const current = existsSync(output) ? readFileSync(output, 'utf8') : undefined;
  if (check) {
    if (current !== contents) {
      return fail({
        code: 'OUT_OF_DATE',
        message: `${path} is out of date. Run the command again without --check to update it.`,
      });
    }
    return {path, upToDate: true};
  }
  if (current !== contents) {
    writeFileSync(output, contents);
  }
  return {path, written: current !== contents};
}

const camelOption = /^--(schemaFile|schemaName|typePrefix)(=|$)/;
const upperCase = /[A-Z]/g;

/**
 * incur reserves `--schema` (it prints a command's JSON Schema), so codegen's
 * schema module is `--schema-file`. This keeps 0.1.0's `codegen --schema
 * <path>` working; a bare `--schema` still prints the JSON Schema. The
 * options are kebab-case keys, so help examples print as typed; this also
 * keeps the camelCase spellings that worked before 0.4.3.
 */
export function legacyArgv(argv: readonly string[]): string[] {
  if (argv[0] !== 'codegen') return [...argv];
  const out: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i]!;
    const next = argv[i + 1];
    if (token === '--schema' && next !== undefined && !next.startsWith('-')) {
      out.push('--schema-file', next);
      i++;
    } else if (token.startsWith('--schema=')) {
      out.push('--schema-file', token.slice('--schema='.length));
    } else if (camelOption.test(token)) {
      const [name, ...value] = token.split('=');
      const kebab = name!.replace(upperCase, c => `-${c.toLowerCase()}`);
      out.push([kebab, ...value].join('='));
    } else {
      out.push(token);
    }
  }
  return out;
}

const check = z
  .boolean()
  .optional()
  .describe('Fail when the output is out of date instead of writing it');

export const cli = Cli.create('zero-swift', {
  version: pkg.version,
  description:
    "Generates what the Zero Swift client needs from a Zero app's TypeScript schema, queries, and mutators.",
}).command('codegen', {
  description:
    "Generate the Swift row types, schema, and query builder from the app's TypeScript schema. Options override zero-swift.json, which is read when present. See swift/Sources/Zero/Zero.docc/GettingStarted.md in github.com/wevm/zero.",
  options: z.object({
    'config': z
      .string()
      .optional()
      .describe('Config file (default zero-swift.json, when present)'),
    'schema-file': z
      .string()
      .optional()
      .describe('TS schema module (default schema.ts)'),
    'export': z
      .string()
      .optional()
      .describe('Export of the module that holds the schema (default schema)'),
    'out': z
      .string()
      .optional()
      .describe('Swift file to write (default Schema.swift)'),
    'schema-name': z
      .string()
      .optional()
      .describe('Name of the generated ZeroSchema static (default app)'),
    'zql': z
      .string()
      .optional()
      .describe('Name of the generated query builder global (default zql)'),
    'type-prefix': z
      .string()
      .optional()
      .describe('Prefix for every generated type name (default none)'),
    'access': z
      .enum(['public', 'internal'])
      .optional()
      .describe('Access level of the generated code (default public)'),
    check,
  }),
  // incur 0.7.1 prints a boolean example option as `--check true`, so the
  // hint shows a bare `--check` instead.
  examples: [
    {description: 'Generate from zero-swift.json'},
    {
      options: {
        'schema-file': 'shared/schema.ts',
        'out': 'Sources/App/Schema.swift',
      },
      description: 'Generate without a config file',
    },
  ],
  hint: 'In CI, zero-swift codegen --check fails when the output is stale.',
  run: c =>
    reporting(c.error, async () => {
      const cwd = process.cwd();
      const {
        'config': configOption,
        'schema-file': schemaFile,
        'schema-name': schemaName,
        'type-prefix': typePrefix,
        out,
        check,
        ...overrides
      } = c.options;
      const configPath =
        configOption ??
        (existsSync(resolve(cwd, 'zero-swift.json'))
          ? 'zero-swift.json'
          : undefined);
      if (configPath) {
        mustExist(
          resolve(cwd, configPath),
          'CONFIG_NOT_FOUND',
          `Config ${configPath} doesn't exist.`,
        );
      }
      const fileConfig = configPath
        ? (JSON.parse(readFileSync(resolve(cwd, configPath), 'utf8')) as Config)
        : {};
      const config: Config = {
        ...fileConfig,
        ...Object.fromEntries(
          Object.entries({...overrides, schemaName, typePrefix}).filter(
            ([, v]) => v !== undefined,
          ),
        ),
        ...(schemaFile ? {schema: schemaFile} : {}),
        ...(out ? {output: out} : {}),
      };
      const swift = await codegenFile(config, cwd);
      return writeOutput(
        c.error,
        resolve(cwd, config.output ?? 'Schema.swift'),
        swift,
        check ?? false,
        cwd,
      );
    }),
});

cli.command(
  Cli.create('drift', {
    description:
      'Record what the TS queries and mutators do, for the Swift drift test (Drift.failures in ZeroTesting).',
  }).command('record', {
    description:
      "Run the app's TS queries and mutators on the samples a module exports, and write what they did.",
    options: z.object({
      samples: z
        .string()
        .optional()
        .describe('Samples module (default drift-samples.ts)'),
      out: z
        .string()
        .optional()
        .describe('Vectors file to write (default drift-vectors.json)'),
      check,
    }),
    examples: [
      {
        options: {
          samples: 'drift-samples.ts',
          out: 'Tests/AppTests/drift-vectors.json',
        },
        description: 'Record the drift vectors',
      },
    ],
    run: c =>
      reporting(c.error, async () => {
        const cwd = process.cwd();
        const samples = resolve(cwd, c.options.samples ?? 'drift-samples.ts');
        mustExist(
          samples,
          'SAMPLES_NOT_FOUND',
          `Samples file ${relative(cwd, samples)} doesn't exist. Pass --samples.`,
        );
        // Loaded here: the recorder runs the app's `@rocicorp/zero`, which
        // codegen does not need.
        const {recordDriftFile} = await import('./drift.ts');
        const vectors = await recordDriftFile(samples);
        return writeOutput(
          c.error,
          resolve(cwd, c.options.out ?? 'drift-vectors.json'),
          vectors,
          c.options.check ?? false,
          cwd,
        );
      }),
  }),
);
