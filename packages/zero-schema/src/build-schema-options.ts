import * as v from '../../shared/src/valita.js';

export const ZERO_BUILD_SCHEMA_ENV_VAR_PREFIX = 'ZERO_';

export const buildSchemaOptions = {
  schema: {
    path: {
      type: v.string().default('schema.ts'),
      desc: [
        'Relative path to the file containing the schema definition.',
        'The file must have a default export of type SchemaConfig.',
      ],
      alias: 'p',
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
    },
  },
};
