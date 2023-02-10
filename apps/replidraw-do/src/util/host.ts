// The websocket address will be ws:// when developing locally or wss://
// in production. Likewise the reflect server is accessible via http://
// in development and https:// in production.
//
// Note: process.env has to be used instead of importing env because Next.js
// apparently does string substitution.
export const workerWsURI =
  process.env.NEXT_PUBLIC_WORKER_HOST ??
  "wss://replidraw-offline.replicache.workers.dev";
export const workerURL = workerWsURI.replace(/^ws/, "http");
