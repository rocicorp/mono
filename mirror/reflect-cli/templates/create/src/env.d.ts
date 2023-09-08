/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_REFLECT_URL: string;
  readonly VITE_ROOM_ID: string;
  // more env variables...
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
