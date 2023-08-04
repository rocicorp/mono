import {describe, expect, test} from '@jest/globals';
import type {Storage} from 'firebase-admin/storage';
import type {Module, ModuleRef} from 'mirror-schema/src/module.js';
import {ModuleAssembler} from './module-assembler.js';
import type {CfModule} from './create-worker-upload-form.js';

describe('module-assembler', () => {
  const serverModules: (Module & ModuleRef)[] = [
    {
      name: 'server.js',
      type: 'esm',
      url: 'gs://reflect-modules/server.js',
      content: 'bonk',
    },
    {
      name: 'worker.template.js',
      type: 'esm',
      url: 'gs://reflect-modules/worker.template.js',
      content: 'import "<REFLECT_SERVER>"; import "<APP>"; do { the thing };',
    },
  ];
  type Case = {
    name: string;
    appModules: (Module & ModuleRef)[];
    expectedModules: CfModule[];
  };
  const cases: Case[] = [
    {
      name: 'all unique names',
      appModules: [
        {
          name: 'index.js',
          type: 'esm',
          content: 'foo bar',
          url: 'gs://reflect-modules/index.js',
        },
      ],
      expectedModules: [
        {
          name: 'worker.js',
          type: 'esm',
          content: 'import "server.js"; import "index.js"; do { the thing };',
        },
        {
          name: 'index.js',
          type: 'esm',
          content: 'foo bar',
        },
        {
          name: 'server.js',
          type: 'esm',
          content: 'bonk',
        },
      ],
    },
    {
      name: 'name collisions',
      appModules: [
        {
          name: 'worker.js',
          type: 'esm',
          content: 'bar baz',
          url: 'gs://reflect-modules/app-worker.js',
        },
        {
          name: 'server.js',
          type: 'esm',
          content: 'foo bar',
          url: 'gs://reflect-modules/app-server.js',
        },
      ],
      expectedModules: [
        {
          name: 'worker0.js',
          type: 'esm',
          content: 'import "server0.js"; import "worker.js"; do { the thing };',
        },
        {
          name: 'worker.js',
          type: 'esm',
          content: 'bar baz',
        },
        {
          name: 'server.js',
          type: 'esm',
          content: 'foo bar',
        },
        {
          name: 'server0.js',
          type: 'esm',
          content: 'bonk',
        },
      ],
    },
    {
      name: 'multiple name collisions',
      appModules: [
        {
          name: 'worker.js',
          type: 'esm',
          content: 'bar baz',
          url: 'gs://reflect-modules/app-worker.js',
        },
        {
          name: 'worker0.js',
          type: 'esm',
          content: 'boo bonk',
          url: 'gs://reflect-modules/app-worker0.js',
        },
        {
          name: 'server.js',
          type: 'esm',
          content: 'foo bar',
          url: 'gs://reflect-modules/app-server.js',
        },
        {
          name: 'server0.js',
          type: 'esm',
          content: 'food bard',
          url: 'gs://reflect-modules/app-server0.js',
        },
      ],
      expectedModules: [
        {
          name: 'worker1.js',
          type: 'esm',
          content: 'import "server1.js"; import "worker.js"; do { the thing };',
        },
        {
          name: 'worker.js',
          type: 'esm',
          content: 'bar baz',
        },
        {
          name: 'worker0.js',
          type: 'esm',
          content: 'boo bonk',
        },
        {
          name: 'server.js',
          type: 'esm',
          content: 'foo bar',
        },
        {
          name: 'server0.js',
          type: 'esm',
          content: 'food bard',
        },
        {
          name: 'server1.js',
          type: 'esm',
          content: 'bonk',
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const storage = {
        bucket: (bucketName: string) => {
          expect(bucketName).toBe('reflect-modules');
          return {
            file: (filename: string) => ({
              // eslint-disable-next-line require-await
              get: async () => [
                {
                  // eslint-disable-next-line require-await
                  download: async () => {
                    for (const m of [...serverModules, ...c.appModules]) {
                      if (m.url.endsWith('/' + filename)) {
                        return [m.content];
                      }
                    }
                    throw new Error(`unknown filename ${filename}`);
                  },
                },
              ],
            }),
          };
        },
      } as unknown as Storage;

      const assembler = new ModuleAssembler(c.appModules, serverModules);
      expect(await assembler.assemble(storage)).toEqual(c.expectedModules);
    });
  }
});
