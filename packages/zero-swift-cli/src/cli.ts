/**
 * `zero-swift codegen`: generates the Swift row types, schema, and query
 * builder of a Zero app from its TypeScript schema.
 *
 *   zero-swift codegen [--schema schema.ts] [--out Schema.swift]
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
 * writing it.
 */
import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import {relative, resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {parseArgs} from 'node:util';
import type {Schema} from '../../zero-types/src/schema.ts';
import {generate, type Config} from './generate.ts';
import {readCustomTypes} from './types.ts';

const usage = `Usage:
  zero-swift codegen [--schema schema.ts] [--out Schema.swift]
                     [--config zero-swift.json] [--check]
  zero-swift drift record [--samples drift-samples.ts]
                          [--out drift-vectors.json] [--check]

codegen generates Swift from the app's TypeScript schema. Options override
zero-swift.json, which is read when present.
drift record runs the app's TS queries and mutators on samples and records
what they did, for the Swift drift test.
--check fails when the output is out of date instead of writing it.

Docs: https://github.com/wevm/zero/tree/main/packages/zero-swift-cli
`;

/** A mistake in how the CLI was run: printed without a stack. */
export class UsageError extends Error {}

function mustExist(path: string, message: string) {
  if (!existsSync(path)) {
    throw new UsageError(message);
  }
}

export async function codegenFile(config: Config, cwd: string) {
  const schemaPath = resolve(cwd, config.schema ?? 'schema.ts');
  mustExist(
    schemaPath,
    `Schema ${relative(cwd, schemaPath)} doesn't exist. Pass --schema, or set "schema" in zero-swift.json.`,
  );
  const exportName = config.export ?? 'schema';
  const module = (await import(pathToFileURL(schemaPath).href)) as Record<
    string,
    unknown
  >;
  const schema = module[exportName] as Schema | undefined;
  if (!schema || typeof schema !== 'object' || !('tables' in schema)) {
    throw new UsageError(
      `${schemaPath} does not export a schema as "${exportName}"`,
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

export async function main(argv: string[]): Promise<number> {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        schema: {type: 'string'},
        out: {type: 'string'},
        config: {type: 'string'},
        samples: {type: 'string'},
        check: {type: 'boolean'},
        help: {type: 'boolean', short: 'h'},
      },
    });
  } catch (e) {
    process.stderr.write(`${(e as Error).message}\n\n${usage}`);
    return 2;
  }
  const {values, positionals} = parsed;
  const [command = 'codegen', subcommand] = positionals;
  if (values.help || command === 'help') {
    process.stdout.write(usage);
    return 0;
  }
  const cwd = process.cwd();
  if (command === 'drift' && subcommand === 'record') {
    const samples = resolve(cwd, values.samples ?? 'drift-samples.ts');
    mustExist(
      samples,
      `Samples file ${relative(cwd, samples)} doesn't exist. Pass --samples.`,
    );
    // Loaded here: the recorder runs the app's `@rocicorp/zero`, which
    // codegen does not need.
    const {recordDriftFile} = await import('./drift.ts');
    const vectors = await recordDriftFile(samples);
    return writeOutput(
      resolve(cwd, values.out ?? 'drift-vectors.json'),
      vectors,
      values.check ?? false,
      cwd,
    );
  }
  if (command !== 'codegen') {
    process.stderr.write(
      `Unknown command "${positionals.join(' ')}"\n\n${usage}`,
    );
    return 2;
  }
  const configPath =
    values.config ??
    (existsSync(resolve(cwd, 'zero-swift.json'))
      ? 'zero-swift.json'
      : undefined);
  if (configPath) {
    mustExist(resolve(cwd, configPath), `Config ${configPath} doesn't exist.`);
  }
  const fileConfig = configPath
    ? (JSON.parse(readFileSync(resolve(cwd, configPath), 'utf8')) as Config)
    : {};
  const config: Config = {
    ...fileConfig,
    ...(values.schema ? {schema: values.schema} : {}),
    ...(values.out ? {output: values.out} : {}),
  };
  const output = resolve(cwd, config.output ?? 'Schema.swift');
  const swift = await codegenFile(config, cwd);
  return writeOutput(output, swift, values.check ?? false, cwd);
}

/** Writes `contents`, or with `check`, fails when the file differs. */
function writeOutput(
  output: string,
  contents: string,
  check: boolean,
  cwd: string,
): number {
  const current = existsSync(output) ? readFileSync(output, 'utf8') : undefined;
  if (check) {
    if (current !== contents) {
      process.stderr.write(`${relative(cwd, output)} is out of date\n`);
      return 1;
    }
    return 0;
  }
  if (current !== contents) {
    writeFileSync(output, contents);
  }
  process.stdout.write(`Wrote ${relative(cwd, output)}\n`);
  return 0;
}
