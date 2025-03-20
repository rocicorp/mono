import process from 'node:process';
import {createInterface} from 'node:readline';
import {format, resolveConfig} from 'prettier';
import {astToZQL} from '../../packages/zql/src/query/ast-to-zql.ts';

function readFromStdin(): Promise<string> {
  return new Promise(resolve => {
    let data = '';
    const rl = createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: false,
    });

    rl.on('line', line => {
      data += line + '\n';
    });

    rl.on('close', () => {
      resolve(data);
    });
  });
}

async function formatOutput(content: string): Promise<string> {
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
    // Return unformatted content if prettier fails
    return content;
  }
}

async function main(): Promise<void> {
  try {
    // Read input from stdin
    const input = await readFromStdin();

    // Parse the input as JSON
    const ast = JSON.parse(input);

    // Process the AST
    const zql = astToZQL(ast);
    const code = `query.${ast.table}${zql}`;

    // Output the result

    // eslint-disable-next-line no-console
    console.log(await formatOutput(code));
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('Error processing input:', error);
    process.exit(1);
  }
}

// Execute the program
await main();
