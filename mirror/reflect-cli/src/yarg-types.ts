import type {ArgumentsCamelCase, Argv} from 'yargs';

/**
 * Yargs options included in every reflect cli command.
 */
export interface CommonYargsOptions {
  v: boolean | undefined;
  ['auth-key-from-env']: string | undefined;
  stack: string;
  local: boolean;
  runAs: string | undefined;
}

export type CommonYargsArgv = Argv<CommonYargsOptions>;

export type YargvToInterface<T> = T extends Argv<infer P>
  ? ArgumentsCamelCase<P>
  : never;
