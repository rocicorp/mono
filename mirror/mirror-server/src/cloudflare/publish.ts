import {
  CfModule,
  CfVars,
  createScriptUploadForm,
} from 'cloudflare-api/src/create-script-upload-form.js';
import type {Script} from 'cloudflare-api/src/scripts.js';
import {Migration, getMigrationsToUpload} from './get-migrations-to-upload.js';

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

  const form = createScriptUploadForm({
    name: script.name,
    main: mainModule, // await createCfModule('worker.js'),
    bindings: {
      vars,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      durable_objects: {
        bindings: [
          // eslint-disable-next-line @typescript-eslint/naming-convention
          {name: 'roomDO', class_name: 'RoomDO'},
          // eslint-disable-next-line @typescript-eslint/naming-convention
          {name: 'authDO', class_name: 'AuthDO'},
        ],
      },
    },
    modules,
    migrations: cfMigrations,
    tags,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    compatibility_date: '2023-05-18',
  });

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
