/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
