import {listVars, ListVarsResponse} from 'mirror-protocol/src/vars.js';
import color from 'picocolors';
import {listDevVars} from '../dev/vars.js';
import type {AuthContext} from '../handler.js';
import {makeRequester} from '../requester.js';
import {padColumns} from '../table.js';
import type {YargvToInterface} from '../yarg-types.js';
import type {CommonVarsYargsArgv} from './types.js';
import {getAppID, getDefaultApp} from '../app-config.js';
import {getLogger} from '../logger.js';

export function listVarsOptions(yargs: CommonVarsYargsArgv) {
  return yargs
    .option('show', {
      desc: 'Show the decrypted environment variables',
      type: 'boolean',
      default: false,
    })
    .option('app', {
      describe: 'The name of the App',
      type: 'string',
      requiresArg: true,
      default: getDefaultApp(),
      required: true,
    });
}

type ListVarsHandlerArgs = YargvToInterface<ReturnType<typeof listVarsOptions>>;

export async function listVarsHandler(
  yargs: ListVarsHandlerArgs,
  authContext: AuthContext,
): Promise<void> {
  const {
    show,
    dev,
    $0: command,
    _: [subcommand],
  } = yargs;
  let response: ListVarsResponse;
  if (dev) {
    response = {
      success: true,
      decrypted: true,
      envs: {
        dev: {
          name: 'Local dev',
          vars: listDevVars(),
        },
      },
    };
  } else {
    const {app} = yargs;
    const {userID} = authContext.user;
    const appID = await getAppID(authContext, app, false);
    const data = {requester: makeRequester(userID), appID, decrypted: show};

    response = await listVars.call(data);
  }
  for (const env of Object.values(response.envs)) {
    const entries = Object.entries(env.vars);
    if (entries.length === 0) {
      getLogger().log(
        `No environment variables set. Use '${command} ${subcommand} set${
          dev ? ' --dev' : ''
        }' to add them.`,
      );
      continue;
    }
    const name = env.name ?? 'Live';
    if (!response.decrypted) {
      entries.forEach(entry => {
        entry[1] = color.italic(color.gray('Encrypted'));
      });
      getLogger().log(
        `\n${name} environment variables (use --show to see their values):\n`,
      );
    } else {
      getLogger().log(`\n${name} environment variables:\n`);
    }

    const lines = padColumns([['name', 'value'], ...entries]);
    lines.forEach(([key, value], i) => {
      if (i === 0) {
        // Header row
        getLogger().log(`${color.gray(key)}     ${color.gray(value)}`);
      } else {
        getLogger().log(`${color.bold(key)}     ${value}`);
      }
    });
  }
}
