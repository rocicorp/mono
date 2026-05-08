import {fileURLToPath} from 'node:url';
import {describePackagesApiSnapshots} from 'tsnapi/vitest';

describePackagesApiSnapshots({
  packages: [
    fileURLToPath(new URL('../../packages/zero', import.meta.url)),
    fileURLToPath(new URL('../../packages/replicache', import.meta.url)),
    fileURLToPath(new URL('../../packages/zero-events', import.meta.url)),
  ],
});
