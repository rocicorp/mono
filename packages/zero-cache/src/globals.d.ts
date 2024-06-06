/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/ban-types */

interface Module {}

type Exports = Record<string, ExportValue>;
type ImportValue = ExportValue | number;
type Imports = Record<string, ModuleImports>;
type ModuleImports = Record<string, ImportValue>;

interface Table {
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Table/length) */
  readonly length: number;
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Table/get) */
  get(index: number): any;
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Table/grow) */
  grow(delta: number, value?: any): number;
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Table/set) */
  set(index: number, value?: any): void;
}

interface Memory {
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Memory/buffer) */
  readonly buffer: ArrayBuffer;
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Memory/grow) */
  grow(delta: number): number;
}

type ExportValue = Function | Global | Memory | Table;

type Imports = Record<string, ModuleImports>;

interface Instance {
  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/Instance/exports) */
  readonly exports: Exports;
}

declare namespace WebAssembly {
  interface WebAssemblyInstantiatedSource {
    instance: Instance;
    module: Module;
  }

  /** [MDN Reference](https://developer.mozilla.org/docs/WebAssembly/JavaScript_interface/instantiate_static) */
  function instantiate(
    bytes: BufferSource,
  ): Promise<WebAssemblyInstantiatedSource>;
  function instantiate(
    moduleObject: Module,
    importObject?: Imports,
  ): Promise<Instance>;
}
