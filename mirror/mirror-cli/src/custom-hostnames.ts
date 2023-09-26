import {getProviderConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {CustomHostnames} from 'cloudflare-api/src/custom-hostnames.js';

export function customHostnamesOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('pattern', {
      desc: 'Optional pattern to match when listing or deleting records',
      type: 'string',
      conflicts: 'create',
    })
    .option('get', {
      desc: 'Gets a custom hostname by ID',
      type: 'string',
      conflicts: ['pattern', 'delete', 'create'],
    })
    .option('create', {
      desc: 'Creates a custom hostname',
      type: 'string',
      conflicts: ['pattern', 'delete', 'get'],
    })
    .option('delete', {
      desc: 'Delete matching records',
      type: 'boolean',
      conflicts: ['create', 'get'],
    });
}

type CustomHostnamesHandlerArgs = YargvToInterface<
  ReturnType<typeof customHostnamesOptions>
>;

export async function customHostnamesHandler(
  yargs: CustomHostnamesHandlerArgs,
): Promise<void> {
  const {pattern = '', delete: deleteHostnames = false, create, get} = yargs;
  const config = await getProviderConfig(yargs);
  const {
    apiToken,
    defaultZone: {zoneID},
  } = config;

  const resource = new CustomHostnames({apiToken, zoneID});

  if (get) {
    const result = await resource.get(get);
    console.log(result);
    return;
  }

  if (create) {
    /* eslint-disable @typescript-eslint/naming-convention */
    const ch = await resource.create({
      hostname: create,
      custom_metadata: {
        script_name: 'loving-fourth-lime-lm9xrdbk', // TODO: Remove experimental code.
      },
      ssl: {
        type: 'dv',
        method: 'http',
        settings: {
          min_tls_version: '1.0',
        },
      },
    });
    /* eslint-enable @typescript-eslint/naming-convention */
    console.log(ch);
    return;
  }

  for (const ch of await resource.list()) {
    if (ch.hostname.indexOf(pattern) >= 0) {
      console.log(`Matched ${ch.hostname}`, ch);
      if (deleteHostnames) {
        const result = await resource.delete(ch.id);
        console.log(`Delete result`, result);
      }
    }
  }
}
