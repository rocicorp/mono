import {listVars} from 'mirror-protocol/src/vars.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {listDevVars} from '../dev/vars.js';
import {makeRequester} from '../requester.js';
import type {YargvToInterface} from '../yarg-types.js';
import type {CommonVarsYargsArgv} from './types.js';

export function listVarsOptions(yargs: CommonVarsYargsArgv) {
  return yargs.option('decrypted', {
    desc: 'Show the decrypted Server Variables',
    type: 'boolean',
    default: false,
  });
}

type ListVarsHandlerArgs = YargvToInterface<ReturnType<typeof listVarsOptions>>;

export async function listVarsHandler(
  yargs: ListVarsHandlerArgs,
): Promise<void> {
  const {decrypted, dev} = yargs;
  let vars: Record<string, string>;
  if (dev) {
    vars = listDevVars();
  } else {
    const {userID} = await authenticate(yargs);
    const {appID} = await ensureAppInstantiated(yargs);
    const data = {requester: makeRequester(userID), appID, decrypted};

    const response = await listVars(data);
    vars = response.vars;
  }
  const varType = dev ? 'Dev' : 'Server';
  const entries = Object.entries(vars);
  if (entries.length === 0) {
    console.log(
      `No ${varType} Variables set. Use 'npx @rocicorp/reflect vars set${
        dev ? ' --dev' : ''
      }' to add them.`,
    );
  } else if (dev) {
    console.log(`Dev Variables:\n`);
  } else if (decrypted) {
    console.log(`Requested decrypted Server Variables:\n`);
  } else {
    console.log(
      'Server Variables are encrypted at rest. Use --decrypted to see their values.\n',
    );
  }
  entries.forEach(([name, value]) => console.log(`${name}=${value}`));
}
