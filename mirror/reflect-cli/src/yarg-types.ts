import type {ArgumentsCamelCase, Argv} from 'yargs';

/**
 * Yargs options included in every reflect cli command.
 */
export interface CommonYargsOptions {
  v: boolean | undefined;
  stack: string;
  runAs: string | undefined;
}

export type CommonYargsArgv = Argv<CommonYargsOptions>;

export type YargvToInterface<T> = T extends Argv<infer P>
  ? ArgumentsCamelCase<P>
  : never;
