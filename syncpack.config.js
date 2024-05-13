/** @type {import('syncpack').RcFile} */
const config = {
  versionGroups: [
    {
      dependencies: ['vitest'],
      packages: ['zero-cache'],
      label:
        '@cloudflare/vitest-pool-workers only works with Vitest 1.3.0: https://developers.cloudflare.com/workers/testing/vitest-integration/get-started/write-your-first-test/#install-vitest-and-cloudflarevitest-pool-workers',
    },
  ],
};
export {config as default};
