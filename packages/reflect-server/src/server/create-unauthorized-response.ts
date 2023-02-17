export function createUnauthorizedResponse(message = 'Unauthorized'): Response {
  return new Response(message, {
    status: 401,
  });
}
