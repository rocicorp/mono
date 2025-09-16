/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-base-to-string, @typescript-eslint/restrict-template-expressions */
function createRandomIdentifier(name: string): string {
  return `${name}_${Math.random() * 10000}`.replace('.', '');
}

/**
 * Injects a global `require` function into the bundle. This is sometimes needed
 * if the dependencies are incorrectly using require.
 */
export function injectRequire(): string {
  const createRequireAlias = createRandomIdentifier('createRequire');
  return `import {createRequire as ${createRequireAlias}} from 'node:module';
var require = ${createRequireAlias}(import.meta.url);
`;
}
