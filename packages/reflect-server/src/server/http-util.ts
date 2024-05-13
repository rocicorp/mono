export function roomNotFoundResponse() {
  return new Response('room not found', {status: 404});
}
