import {
  CfModule,
  CfVars,
  createScriptUploadForm,
} from 'cloudflare-api/src/create-script-upload-form.js';
import type {Script} from 'cloudflare-api/src/scripts.js';
import {Migration, getMigrationsToUpload} from './get-migrations-to-upload.js';
import {TAIL_WORKERS} from 'mirror-workers/src/service-names.js';
import {logger} from 'firebase-functions';

export async function uploadScript(
  script: Script,
  mainModule: CfModule,
  modules: CfModule[],
  vars: CfVars,
  tags: string[],
) {
  const cfMigrations = await getMigrationsToUpload(script, {
    config: {migrations},
  });

  const tailWorkers = TAIL_WORKERS.map(service => ({service}));

  const form = createScriptUploadForm({
    /* eslint-disable @typescript-eslint/naming-convention */
    name: script.name,
    main: mainModule,
    bindings: {
      vars,
      durable_objects: {
        bindings: [
          {name: 'roomDO', class_name: 'RoomDO'},
          {name: 'authDO', class_name: 'AuthDO'},
        ],
      },
    },
    modules,
    migrations: cfMigrations,
    tags,
    tail_consumers: tailWorkers,
    compatibility_date: '2023-05-18',
    compatibility_flags: ['nodejs_compat'],
    /* eslint-enable @typescript-eslint/naming-convention */
  });

  try {
    logger.debug(`Worker Metadata`, JSON.parse(String(form.get('metadata'))));
  } catch (e) {
    logger.warn(`Could not log Worker Metadata`, e);
  }

  const searchParams = new URLSearchParams({
    // eslint-disable-next-line @typescript-eslint/naming-convention
    include_subdomain_availability: 'true',
    // pass excludeScript so the whole body of the
    // script doesn't get included in the response
    excludeScript: 'true',
  });
  await script.upload(form, searchParams);
}

const migrations: Migration[] = [
  {
    tag: 'v1',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    new_classes: ['RoomDO', 'AuthDO'],
  },
];
