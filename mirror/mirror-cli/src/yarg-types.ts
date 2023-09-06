import type {ArgumentsCamelCase, Argv} from 'yargs';

/**
 * Yargs options included in every reflect cli command.
 */
export interface CommonYargsOptions {
  stack: string;
}

export type CommonYargsArgv = Argv<CommonYargsOptions>;

export type YargvToInterface<T> = T extends Argv<infer P>
  ? ArgumentsCamelCase<P>
  : never;
