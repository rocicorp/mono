function createRandomIdentifier(name) {
  return `${name}_${Math.random() * 10000}`.replace('.', '');
}

/**
 * Injects a global `require` function into the bundle. This is sometimes needed
 * if the dependencies are incorrectly using require.
 *
 * @returns {esbuild.BuildOptions}
 */
export function injectRequire() {
  const createRequireAlias = createRandomIdentifier('createRequire');
  return `import {createRequire as ${createRequireAlias}} from 'node:module';
var require = ${createRequireAlias}(import.meta.url);
`;
}
