import {getFirestore} from 'firebase/firestore';
import {setVars} from 'mirror-protocol/src/vars.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {setDevVars} from '../dev/vars.js';
import {UserError} from '../error.js';
import {password} from '../inquirer.js';
import {makeRequester} from '../requester.js';
import {watchDeployment} from '../watch-deployment.js';
import type {YargvToInterface} from '../yarg-types.js';
import type {CommonVarsYargsArgv} from './types.js';

export function setVarsOptions(yargs: CommonVarsYargsArgv) {
  return yargs.positional('keysAndValues', {
    describe:
      'Space-separated KEY=VALUE pairs, or KEY only to input its VALUE with a password prompt',
    type: 'string',
    array: true,
    demandOption: true,
  });
}

type SetVarsHandlerArgs = YargvToInterface<ReturnType<typeof setVarsOptions>>;

export async function setVarsHandler(yargs: SetVarsHandlerArgs): Promise<void> {
  const {keysAndValues, dev} = yargs;

  const vars: Record<string, string> = {};
  for (const kv of keysAndValues) {
    const eq = kv.indexOf('=');
    if (eq === 0) {
      throw new UserError(`Malformed KEY=VALUE pair "${kv}"`);
    }
    const key = eq > 0 ? kv.substring(0, eq) : kv;
    if (vars[key]) {
      throw new UserError(`Duplicate entries for KEY "${key}"`);
    }
    const value =
      eq > 0
        ? kv.substring(eq + 1)
        : await password({
            message: `Enter the value for ${key}:`,
          });
    vars[key] = value;
  }

  if (dev) {
    setDevVars(vars);
    console.log('Set Dev Variables');
    return;
  }

  const {userID} = await authenticate(yargs);
  const {appID} = await ensureAppInstantiated(yargs);
  const data = {requester: makeRequester(userID), appID, vars};
  const {deploymentPath} = await setVars(data);
  if (!deploymentPath) {
    console.log('Stored encrypted Server Variables');
  } else {
    console.log('Deploying updated Server Variables');
    await watchDeployment(getFirestore(), deploymentPath, 'Deployed');
  }
}
