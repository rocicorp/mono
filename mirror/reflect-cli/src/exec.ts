import {execSync, ExecSyncOptions} from 'node:child_process';
import {ErrorWrapper} from './error.js';

export function execOrReportWarning(
  command: string,
  options: ExecSyncOptions,
): string | Buffer {
  try {
    return execSync(command, options);
  } catch (e) {
    throw new ErrorWrapper(e, 'WARNING');
  }
}
