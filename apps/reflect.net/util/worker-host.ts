export function getWorkerHost(): string {
  return (
    process.env.NEXT_PUBLIC_WORKER_HOST ||
    `${location.protocol}//${location.hostname}:8787`
  ).replace(/^ws/, 'http');
}
