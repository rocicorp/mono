import {styleText} from 'node:util';
import {createJwkPair} from '../auth/jwt.ts';

const {privateJwk, publicJwk} = await createJwkPair();
// oxlint-disable-next-line no-console
console.log(
  styleText('red', 'PRIVATE KEY:\n\n'),
  JSON.stringify(privateJwk),
  styleText('green', '\n\nPUBLIC KEY:\n\n'),
  JSON.stringify(publicJwk),
);
