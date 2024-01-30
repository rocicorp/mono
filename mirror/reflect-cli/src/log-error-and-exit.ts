import color from 'picocolors';

export function logErrorAndExit(
  message: string,
  format: (input: string) => string = color.red,
): never {
  console.log(format(message));
  throw new Error(message);
  //process.exit(1);
}

export const noFormat = (input: string) => input;
