import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import {writeFile} from 'node:fs/promises';
import {authorizationConfigSchema} from './compiled-authorization.js';
import * as v from '../../shared/src/valita.js';
import {parseOptions} from '../../shared/src/options.js';
import {isSchemaConfig} from './schema-config.js';

export const schemaOptions = {
  path: {
    type: v.string().default('schema.ts'),
    desc: [
      'Relative path to the file containing the schema definition.',
      'The file must have a default export of type:',
      '  schema: Schema,',
      '  authorization?: CompiledAuthorizationConfig | undefined',
    ],
    alias: 'p',
  },
  output: {
    type: v.string().default('zero-schema.json'),
    desc: [
      'Output path for the generated schema JSON file.',
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
    if (!isSchemaConfig(module.default)) {
      throw new Error(
        'Schema file must have a default export of type SchemaConfig.',
      );
    }
    const schemaConfig = module.default;
    const authConfig = v.parse(
      await schemaConfig.authorization,
      authorizationConfigSchema,
    );

    const output = {
      authorization: authConfig,
      schema: schemaConfig.schema,
    };

    await writeFile(config.output, JSON.stringify(output, undefined, 2));
  } catch (e) {
    console.error(`Failed to load zero schema from ${absoluteConfigPath}:`, e);
    process.exit(1);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
