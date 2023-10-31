import {ENCRYPTION_KEY_SECRET_NAME} from 'mirror-schema/src/app.js';
import crypto from 'node:crypto';
import {storeSecret} from './secrets.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function genEncryptionKeyOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('bits', {
      type: 'number',
      default: 256,
    })
    .option('add-new-version', {
      type: 'boolean',
      desc: 'Adds a new version of the secret (i.e. rotation)',
      default: false,
    });
}

type GenEncyptionKeyHandlerArgs = YargvToInterface<
  ReturnType<typeof genEncryptionKeyOptions>
>;

export async function genEncryptionKeyHandler(
  yargs: GenEncyptionKeyHandlerArgs,
) {
  const {stack, bits, addNewVersion} = yargs;
  const buffer = crypto.randomBytes(bits / 8);
  const key = buffer.toString('base64url');
  await storeSecret(stack, ENCRYPTION_KEY_SECRET_NAME, key, addNewVersion);
}
