/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_PUBLIC_SERVER: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
