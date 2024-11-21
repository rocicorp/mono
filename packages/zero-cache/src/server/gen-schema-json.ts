import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import {writeFile} from 'node:fs/promises';
import {assert} from '../../../shared/src/asserts.js';
import {authorizationConfigSchema} from '../../../zero-schema/src/compiled-authorization.js';
import * as v from '../../../shared/src/valita.js';
import {parseOptions} from '../config/config.js';
import type {Schema} from '../../../zero-schema/src/schema.js';

export const schemaOptions = {
  path: {
    type: v.string().default('schema.ts'),
    desc: [
      'Path to the authorization schema file.',
      '',
      'This file must export two items:',
      '- "schema": The schema definition for your tables',
      '- "authorization": The authorization rules for your tables',
    ],
    alias: 'p',
  },
  output: {
    type: v.string().default('zero-schema.json'),
    desc: [
      'Output path for the generated schema file.',
      '',
      'The schema will be written as a JSON file containing the compiled',
      'authorization rules derived from your schema definition.',
    ],
    alias: 'o',
  },
};

async function main() {
  const config = parseOptions(
    schemaOptions,
    process.argv.slice(2),
    'ZERO_SCHEMA_',
  );

  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const absoluteConfigPath = path.resolve(config.path);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  try {
    const module = await tsImport(relativePath, import.meta.url);
    assert(module.default.schema, 'Schema file must export "schema"');
    assert(
      module.default.authorization,
      'Schema file must export "authorization"',
    );
    assert(module.schema, 'Schema file must export "schema type"');

    const authConfig = v.parse(
      await module.default.authorization,
      authorizationConfigSchema,
      'strict',
    );
    const rawSchema = (await module.default.schema) as Schema;
    const output = {
      authorization: authConfig,
      schema: rawSchema,
    };

    await writeFile(config.output, JSON.stringify(output, undefined, 2));
  } catch (e) {
    console.error(
      `Failed to load zero schema from ${absoluteConfigPath}: ${e}`,
    );
    process.exit(1);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
