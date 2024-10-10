import * as esbuild from 'esbuild';

export const replaceNodeUtil: esbuild.Plugin = {
  name: 'replace-node-util',
  setup(build) {
    build.onResolve({filter: /^util$/}, () => {
      return {
        path: 'node:util',
        namespace: 'node-util-replace',
      };
    });
    build.onResolve({filter: /^node:util$/}, () => {
      return {
        path: 'node:util',
        namespace: 'node-util-replace',
      };
    });
    build.onLoad({filter: /.*/, namespace: 'node-util-replace'}, () => {
      return {
        contents: `const gTextEncoder = TextEncoder;
          export default {TextEncoder: gTextEncoder};
          export {gTextEncoder as TextEncoder};`,
      };
    });
  },
};
