import {listVars} from 'mirror-protocol/src/vars.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {listDevVars} from '../dev/vars.js';
import {makeRequester} from '../requester.js';
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
  let vars: Record<string, string>;
  if (dev) {
    vars = listDevVars();
  } else {
    const {userID} = await authenticate(yargs);
    const {appID} = await ensureAppInstantiated(yargs);
    const data = {requester: makeRequester(userID), appID, decrypted: show};

    const response = await listVars(data);
    vars = response.vars;
  }
  const varType = dev ? 'local dev' : 'environment';
  const entries = Object.entries(vars);
  if (entries.length === 0) {
    console.log(
      `No ${varType} variables set. Use '${command} ${subcommand} set${
        dev ? ' --dev' : ''
      }' to add them.`,
    );
  } else if (dev) {
    console.log(`Local dev variables:\n`);
  } else if (show) {
    console.log(`Requested decrypted environment variables:\n`);
  } else {
    console.log(
      'Environment variables are encrypted at rest. Use --show to see their values.\n',
    );
  }
  entries.forEach(([name, value]) => console.log(`${name}=${value}`));
}
