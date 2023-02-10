/**
 * In certain scenarios the server can signal that it does not know about the
 * client. For example, the server might have deleted the client.
 */
export type ClientStateNotFoundResponse = {
  error: 'ClientStateNotFound';
};

export function isClientStateNotFoundResponse(
  result: unknown,
): result is ClientStateNotFoundResponse {
  return (
    typeof result === 'object' &&
    result !== null &&
    (result as Partial<ClientStateNotFoundResponse>).error ===
      'ClientStateNotFound'
  );
}
