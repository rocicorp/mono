/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
import {LogContext} from '@rocicorp/logger';
import type {OnErrorParameters} from './on-error.ts';

// eslint-disable-next-line @typescript-eslint/naming-convention
export const ZeroLogContext = LogContext<OnErrorParameters>;
export type ZeroLogContext = LogContext<OnErrorParameters>;
