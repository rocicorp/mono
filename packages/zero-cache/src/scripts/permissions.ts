import {logOptions} from '../../../otel/src/log-options.ts';
import * as v from '../../../shared/src/valita.ts';
import {appOptions, shardOptions, zeroOptions} from '../config/zero-config.ts';
export {loadSchemaAndPermissions} from 'zero-permissions/src/load-schema.ts';

export const deployPermissionsOptions = {
  schema: {
    path: {
      type: v.string().default('schema.ts'),
      desc: ['Relative path to the file containing the schema definition.'],
      alias: 'p',
    },
  },

  upstream: {
    db: {
      type: v.string().optional(),
      desc: [
        `The upstream Postgres database to deploy permissions to.`,
        `This is ignored if an {bold output-file} is specified.`,
      ],
    },

    type: zeroOptions.upstream.type,
  },

  app: {id: appOptions.id},

  shard: shardOptions,

  log: logOptions,

  output: {
    file: {
      type: v.string().optional(),
      desc: [
        `Outputs the permissions to a file with the requested {bold output-format}.`,
      ],
    },

    format: {
      type: v.literalUnion('sql', 'json', 'pretty').default('sql'),
      desc: [
        `The desired format of the output file.`,
        ``,
        `A {bold sql} file can be executed via "psql -f <file.sql>", or "\\\\i <file.sql>"`,
        `from within the psql console, or copied and pasted into a migration script.`,
        ``,
        `The {bold json} and {bold pretty} formats are available for non-pg backends`,
        `and general debugging.`,
      ],
    },
  },

  force: {
    type: v.boolean().default(false),
    desc: [`Deploy to upstream without validation. Use at your own risk.`],
    alias: 'f',
  },
};
