import * as v from '../../shared/src/valita.js';

const envNamePrefix = 'ZERO_SCHEMA_';

/**
 * envNamePrefix is specified in options to enable
 * merging these options with zero-cache options in
 * the zero-cache-dev script.
 */
export const buildSchemaOptions = {
  path: {
    type: v.string().default('schema.ts'),
    desc: [
      'Relative path to the file containing the schema definition.',
      'The file must have a default export of type SchemaConfig.',
    ],
    alias: 'p',
    envNamePrefix,
  },
  output: {
    type: v.string().default('zero-schema.json'),
    desc: [
      'Output path for the generated schema JSON file.',
      '',
      'The schema will be written as a JSON file containing the compiled',
      'permission rules derived from your schema definition.',
    ],
    alias: 'o',
    envNamePrefix,
  },
};
