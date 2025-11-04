import {LogContext} from '@rocicorp/logger';

export type OnLogParameters = [message: string, ...rest: unknown[]];
export const ZeroLogContext = LogContext<OnLogParameters>;
export type ZeroLogContext = LogContext<OnLogParameters>;
