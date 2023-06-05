import type {ArgumentsCamelCase, Argv, CamelCaseKey} from 'yargs';

// API dev only passes in camel-cased versions of keys, so ensure
// only camel-cased keys are used
export type OnlyCamelCase<T = Record<string, never>> = {
  [key in keyof T as CamelCaseKey<key>]: T[key];
};

/**
 * Yargs options included in every wrangler command.
 */
export interface CommonYargsOptions {
  v: boolean | undefined;
  config: string | undefined;
  env: string | undefined;
}

export type CommonYargsArgv = Argv<CommonYargsOptions>;

export type YargvToInterface<T> = T extends Argv<infer P>
  ? ArgumentsCamelCase<P>
  : never;

// See http://stackoverflow.com/questions/51465182/how-to-remove-index-signature-using-mapped-types
type RemoveIndex<T> = {
  [K in keyof T as string extends K
    ? never
    : number extends K
    ? never
    : K]: T[K];
};

/**
 * Given some Yargs Options function factory, extract the interface
 * that corresponds to the yargs arguments, remove index types, and only allow camelCase
 */
export type StrictYargsOptionsToInterface<
  T extends (yargs: CommonYargsArgv) => Argv,
> = T extends (yargs: CommonYargsArgv) => Argv<infer P>
  ? OnlyCamelCase<RemoveIndex<ArgumentsCamelCase<P>>>
  : never;
