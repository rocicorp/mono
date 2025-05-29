import {format} from 'prettier';

export async function formatOutput(content: string): Promise<string> {
  try {
    return await format(content, {
      parser: 'typescript',
      semi: false,
    });
  } catch (error) {
    // eslint-disable-next-line no-console
    console.warn('Warning: Unable to format output with prettier:', error);
    return content;
  }
}
