import * as path from 'node:path';
import {format, resolveConfig} from 'prettier';

export async function formatOutput(content: string): Promise<string> {
  try {
    // Resolve prettier config from current working directory. If the config
    // file is in the cwd prettier fails to find it so we unconditionally add
    // one more path component. This is because prettier uses the path of the
    // file being formatted to find the config file.
    const options = (await resolveConfig(path.join(process.cwd(), 'z'))) ?? {};
    return await format(content, {
      ...options,
      parser: 'typescript',
      semi: false,
    });
  } catch (error) {
    // eslint-disable-next-line no-console
    console.warn('Warning: Unable to format output with prettier:', error);
    return content;
  }
}
