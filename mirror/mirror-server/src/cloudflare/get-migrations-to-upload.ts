// This is taken from workers-sdk/packages/wrangler/
// TODO(arv): Remove thing we don not need.

/* eslint-disable @typescript-eslint/naming-convention */
import type {CfDurableObjectMigrations} from 'cloudflare-api/src/create-script-upload-form.js';
import type {Script, ScriptState} from 'cloudflare-api/src/scripts.js';
import {ERRORS, FetchResultError} from 'cloudflare-api/src/fetch.js';

export type Migration = {
  /** A unique identifier for this migration. */
  tag: string;
  /** The new Durable Objects being defined. */
  new_classes?: string[] | undefined;
  /** The Durable Objects being renamed. */
  renamed_classes?:
    | {
        from: string;
        to: string;
      }[]
    | undefined;
  /** The Durable Objects being removed. */
  deleted_classes?: string[] | undefined;
};

type Config = {
  migrations: Migration[];
};

/**
 * For a given Worker + migrations config, figure out which migrations
 * to upload based on the current migration tag of the deployed Worker.
 */
export async function getMigrationsToUpload(
  scriptRef: Script,
  props: {
    config: Config;
  },
): Promise<CfDurableObjectMigrations | undefined> {
  const {config} = props;

  // if config.migrations
  let migrations;
  if (config.migrations.length > 0) {
    // get current migration tag
    let script: ScriptState | undefined;
    try {
      // Note: We only support the default 'production' environment for GlobalScripts.
      // For namespaced scripts, there is no notion of environments.
      const scriptData = await scriptRef.productionEnvironment();
      script = scriptData.script;
    } catch (err) {
      FetchResultError.throwIfCodeIsNot(
        err,
        ERRORS.environmentNotFound,
        ERRORS.serviceNotFound,
      );
    }

    if (script?.migration_tag) {
      // was already published once
      const scriptMigrationTag = script.migration_tag;
      const foundIndex = config.migrations.findIndex(
        migration => migration.tag === scriptMigrationTag,
      );
      if (foundIndex === -1) {
        // logger.warn(
        //   `The published script ${scriptName} has a migration tag "${script.migration_tag}, which was not found in wrangler.toml. You may have already deleted it. Applying all available migrations to the script...`,
        // );
        migrations = {
          old_tag: script.migration_tag,
          new_tag: config.migrations[config.migrations.length - 1].tag,
          steps: config.migrations.map(({tag: _tag, ...rest}) => rest),
        } as const;
      } else {
        if (foundIndex !== config.migrations.length - 1) {
          // there are new migrations to send up
          migrations = {
            old_tag: script.migration_tag,
            new_tag: config.migrations[config.migrations.length - 1].tag,
            steps: config.migrations
              .slice(foundIndex + 1)
              .map(({tag: _tag, ...rest}) => rest),
          } as const;
        }
        // else, we're up to date, no migrations to send
      }
    } else {
      // first time publishing durable objects to this script,
      // so we send all the migrations
      migrations = {
        new_tag: config.migrations[config.migrations.length - 1].tag,
        steps: config.migrations.map(({tag: _tag, ...rest}) => rest),
      } as const;
    }
  }

  return migrations;
}
