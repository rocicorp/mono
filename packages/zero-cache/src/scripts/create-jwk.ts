import chalk from 'chalk';
import {createJwkPair} from '../auth/jwt.ts';

const {privateJwk, publicJwk} = await createJwkPair();
// eslint-disable-next-line no-console
console.log(
  chalk.red('PRIVATE KEY:\n\n'),
  JSON.stringify(privateJwk),
  chalk.green('\n\nPUBLIC KEY:\n\n'),
  JSON.stringify(publicJwk),
);
