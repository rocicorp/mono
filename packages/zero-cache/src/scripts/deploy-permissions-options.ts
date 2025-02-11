import * as v from '../../../shared/src/valita.ts';
import {zeroOptions} from '../config/zero-config.ts';

export const deployPermissionsOptions = {
  schema: {
    path: {
      type: v.string().default('schema.ts'),
      desc: [
        'Relative path to the file containing the schema definition.',
        'The file must have a default export of type SchemaConfig.',
      ],
      alias: 'p',
    },
  },

  upstream: {
    db: {
      ...zeroOptions.upstream.db,
      type: v.string().optional(),
      desc: [
        `The upstream Postgres database to deploy permissions to.`,
        `This is ignored if an {bold output-file} is specified.`,
      ],
    },
  },

  output: {
    file: {
      type: v.string().optional(),
      desc: [
        `Outputs the permissions to a file with the requested {bold output-format}.`,
      ],
    },

    format: {
      type: v.union(v.literal('sql'), v.literal('json')).default('sql'),
      desc: [
        `The desired format of the output file.`,
        ``,
        `A {bold sql} file can be executed via "psql -f <file.sql>", or "\\\\i <file.sql>"`,
        `from within the psql console, or copied and pasted into a migration script.`,
        ``,
        `The {bold json} format is available for general debugging.`,
      ],
    },
  },
};
