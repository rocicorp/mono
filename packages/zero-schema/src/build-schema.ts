import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import {writeFile} from 'node:fs/promises';
import {parseOptions} from '../../shared/src/options.js';
import {stringifySchema} from './schema-config.js';
import {buildSchemaOptions} from './build-schema-options.js';

async function main() {
  const config = parseOptions(buildSchemaOptions, process.argv.slice(2));

  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const absoluteConfigPath = path.resolve(config.path);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  try {
    const module = await tsImport(relativePath, import.meta.url);
    await writeFile(config.output, await stringifySchema(module));
  } catch (e) {
    console.error(`Failed to load zero schema from ${absoluteConfigPath}:`, e);
    process.exit(1);
  }
}

void main();
