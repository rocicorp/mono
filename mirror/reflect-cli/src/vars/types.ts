import type {Argv} from 'yargs';
import type {CommonYargsOptions} from '../yarg-types.js';

export interface CommonVarsYargsOptions extends CommonYargsOptions {
  dev: boolean | undefined;
}

export type CommonVarsYargsArgv = Argv<CommonVarsYargsOptions>;
