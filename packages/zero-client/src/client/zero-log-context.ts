import {LogContext} from '@rocicorp/logger';
import type {OnErrorParameters} from './on-error.ts';

// eslint-disable-next-line @typescript-eslint/naming-convention
export const ZeroLogContext = LogContext<OnErrorParameters>;
export type ZeroLogContext = LogContext<OnErrorParameters>;
