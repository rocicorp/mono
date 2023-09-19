import {describe, expect, test} from '@jest/globals';
import type {Storage} from 'firebase-admin/storage';
import type {Module, ModuleRef} from 'mirror-schema/src/module.js';
import {ModuleAssembler} from './module-assembler.js';
import type {CfModule} from 'cloudflare-api/src/create-script-upload-form.js';

describe('module-assembler', () => {
  const serverModules: (Module & ModuleRef)[] = [
    {
      name: 'server.js',
      type: 'esm',
      url: 'gs://reflect-modules/server.js',
      content: 'bonk',
    },
    {
      name: 'script.template.js',
      type: 'esm',
      url: 'gs://reflect-modules/script.template.js',
      content:
        'import "server-module-name.js"; import "app-module-name.js"; console.log("app-script-name@app-name.team-subdomain");',
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
          name: 'script.js',
          type: 'esm',
          content:
            'import "server.js"; import "index.js"; console.log("my-app-script@my-app-name.my-team-subdomain");',
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
          name: 'script.js',
          type: 'esm',
          content: 'bar baz',
          url: 'gs://reflect-modules/app-script.js',
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
          name: 'script0.js',
          type: 'esm',
          content:
            'import "server0.js"; import "script.js"; console.log("my-app-script@my-app-name.my-team-subdomain");',
        },
        {
          name: 'script.js',
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
          name: 'script.js',
          type: 'esm',
          content: 'bar baz',
          url: 'gs://reflect-modules/app-script.js',
        },
        {
          name: 'script0.js',
          type: 'esm',
          content: 'boo bonk',
          url: 'gs://reflect-modules/app-script0.js',
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
          name: 'script1.js',
          type: 'esm',
          content:
            'import "server1.js"; import "script.js"; console.log("my-app-script@my-app-name.my-team-subdomain");',
        },
        {
          name: 'script.js',
          type: 'esm',
          content: 'bar baz',
        },
        {
          name: 'script0.js',
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

      const assembler = new ModuleAssembler(
        'my-app-name',
        'my-team-subdomain',
        'my-app-script',
        c.appModules,
        serverModules,
      );
      expect(await assembler.assemble(storage)).toEqual(c.expectedModules);
    });
  }
});
