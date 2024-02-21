import color from 'picocolors';
import {getLogger} from './logger.js';

export function logErrorAndExit(
  message: string,
  format: (input: string) => string = color.red,
): never {
  getLogger().error(format(message));
  process.exit(1);
}

export const noFormat = (input: string) => input;
