import {format, resolveConfig} from 'prettier';
export async function formatOutput(content: string): Promise<string> {
  try {
    const options = (await resolveConfig(new URL(import.meta.url))) ?? {};
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
