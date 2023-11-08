import {ENCRYPTION_KEY_SECRET_NAME} from 'mirror-schema/src/env.js';
import crypto from 'node:crypto';
import {assert} from 'shared/src/asserts.js';
import {storeSecret} from './secrets.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function genEncryptionKeyOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('bits', {
      type: 'number',
      desc: 'Size of the key in bits. Must be a multiple of 8.',
      default: 256,
    })
    .option('add-new-version', {
      type: 'boolean',
      desc: 'Adds a new version of the secret (i.e. rotation)',
      default: false,
    });
}

type GenEncryptionKeyHandlerArgs = YargvToInterface<
  ReturnType<typeof genEncryptionKeyOptions>
>;

export async function genEncryptionKeyHandler(
  yargs: GenEncryptionKeyHandlerArgs,
) {
  const {stack, bits, addNewVersion} = yargs;
  assert(bits % 8 === 0);
  const buffer = crypto.randomBytes(bits / 8);
  const key = buffer.toString('base64url');
  await storeSecret(stack, ENCRYPTION_KEY_SECRET_NAME, key, addNewVersion);
}
