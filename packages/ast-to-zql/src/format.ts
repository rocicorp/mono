import {format} from 'oxfmt';

export async function formatOutput(content: string): Promise<string> {
  try {
    const result = await format('output.ts', content, {
      semi: false,
      printWidth: 80,
    });
    return result.code;
  } catch (error) {
    // oxlint-disable-next-line no-console
    console.warn('Warning: Unable to format output with oxfmt:', error);
    return content;
  }
}
