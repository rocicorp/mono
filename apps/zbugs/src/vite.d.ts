/// <reference types="vite/client" />

interface ImportMetaEnv {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  readonly VITE_PUBLIC_SERVER: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
