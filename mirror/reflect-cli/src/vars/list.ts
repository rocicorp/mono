import {Env, listVars} from 'mirror-protocol/src/vars.js';
import color from 'picocolors';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {listDevVars} from '../dev/vars.js';
import {makeRequester} from '../requester.js';
import {padColumns} from '../table.js';
import type {YargvToInterface} from '../yarg-types.js';
import type {CommonVarsYargsArgv} from './types.js';

export function listVarsOptions(yargs: CommonVarsYargsArgv) {
  return yargs.option('show', {
    desc: 'Show the decrypted environment variables',
    type: 'boolean',
    default: false,
  });
}

type ListVarsHandlerArgs = YargvToInterface<ReturnType<typeof listVarsOptions>>;

export async function listVarsHandler(
  yargs: ListVarsHandlerArgs,
): Promise<void> {
  const {
    show,
    dev,
    $0: command,
    _: [subcommand],
  } = yargs;
  let envs: Record<string, Env>;
  if (dev) {
    envs = {
      dev: {
        name: 'Local dev',
        vars: listDevVars(),
      },
    };
  } else {
    const {userID} = await authenticate(yargs);
    const {appID} = await ensureAppInstantiated(yargs);
    const data = {requester: makeRequester(userID), appID, decrypted: show};

    const response = await listVars(data);
    envs = response.envs;
  }
  for (const env of Object.values(envs)) {
    const entries = Object.entries(env.vars);
    if (entries.length === 0) {
      console.log(
        `No variables set. Use '${command} ${subcommand} set${
          dev ? ' --dev' : ''
        }' to add them.`,
      );
      continue;
    }
    const showHelp = dev || show ? '' : ' (use --show to see their values)';
    if (showHelp) {
      entries.forEach(entry => {
        entry[1] = color.italic(color.gray('Encrypted'));
      });
    }
    console.log(`\n${env.name ?? 'Live'} environment variables${showHelp}:\n`);

    const lines = padColumns([['name', 'value'], ...entries]);
    lines.forEach(([key, value], i) => {
      if (i === 0) {
        // Header row
        console.log(`${color.gray(key)}     ${color.gray(value)}`);
      } else {
        console.log(`${color.bold(key)}     ${value}`);
      }
    });
  }
}
