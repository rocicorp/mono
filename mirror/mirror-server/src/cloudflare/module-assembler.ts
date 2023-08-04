import type {Storage} from 'firebase-admin/storage';
import {logger} from 'firebase-functions';
import {FunctionsErrorCode, HttpsError} from 'firebase-functions/v2/https';
import {type ModuleRef, loadModule} from 'mirror-schema/src/module.js';
import {assert} from 'shared/src/asserts.js';
import type {CfModule} from './create-worker-upload-form.js';

export class ModuleAssembler {
  #appModules: ModuleRef[];
  #serverModules: ModuleRef[];
  #uniqueModuleNames: Set<string>;

  constructor(appModules: ModuleRef[], serverModules: ModuleRef[]) {
    assert(appModules.length >= 1);
    assert(serverModules.length === 2); // The current logic only supports the server and template modules.
    this.#appModules = appModules;
    this.#serverModules = serverModules;
    this.#uniqueModuleNames = assertAllModulesHaveUniqueNames(
      appModules,
      'internal', // This should have been validated upstream; an error here would be 'internal'.
    );
  }

  /** Assembles and returns the list of Modules, starting with main (i.e. worker) module. */
  async assemble(storage: Storage): Promise<CfModule[]> {
    const appModuleName = this.#appModules[0].name;
    let serverModuleName = this.#serverModules[0].name;

    const loadedModules = await Promise.all(
      [...this.#appModules, ...this.#serverModules].map(ref =>
        loadModule(storage, ref),
      ),
    );

    const assembled = loadedModules.splice(0, this.#appModules.length);
    for (const m of loadedModules) {
      if (m.name === serverModuleName) {
        serverModuleName = this.#uniquifyAndAddName(serverModuleName);
        assembled.push({...m, name: serverModuleName});
      } else if (m.name === 'worker.template.js') {
        const content = m.content
          .replaceAll('<REFLECT_SERVER>', serverModuleName)
          .replaceAll('<APP>', appModuleName);
        const name = this.#uniquifyAndAddName('worker.js');
        // Main module is the first.
        assembled.unshift({content, name, type: 'esm'});
      } else {
        throw new HttpsError('internal', `Unexpected server module ${m.name}`);
      }
    }
    logger.info(`Assembled modules [${assembled.map(m => m.name)}]`);
    return assembled;
  }

  #uniquifyAndAddName(orig: string): string {
    let name = orig;
    for (let num = 0; this.#uniqueModuleNames.has(name); num++) {
      const parts = orig.split('.');
      name = [`${parts[0]}${num}`, ...parts.slice(1)].join('.');
    }
    this.#uniqueModuleNames.add(name);
    return name;
  }
}

export function assertAllModulesHaveUniqueNames(
  modules: Iterable<{name: string}>,
  errorCode: FunctionsErrorCode = 'invalid-argument',
): Set<string> {
  const names = new Set<string>();
  for (const m of modules) {
    if (names.has(m.name)) {
      throw new HttpsError(errorCode, `Duplicate module name: ${m.name}`);
    }
    names.add(m.name);
  }
  return names;
}
