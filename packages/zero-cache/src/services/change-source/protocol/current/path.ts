/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
/**
 * The path required for a custom Change Source endpoint implementing the
 * Change Source protocol. The version in the path indicates the current
 * (i.e. latest) protocol version of the code, and is the only protocol
 * supported by the code.
 *
 * Eventually, when a backwards incompatible change is made to the protocol,
 * the version will be bumped to ensure that the protocol is only used for an
 * endpoint that explicitly understands it. (While the protocol is in flux
 * and being developed, a starting "v0" version will not follow this
 * convention.)
 *
 * Historic versions are kept in the source code (e.g. v1, v2, etc.) to
 * allow Change Source implementations to import and support multiple
 * versions simultaneously. This is necessary to seamlessly transitioning
 * from a `zero-cache` speaking one version to a `zero-cache` speaking
 * another.
 */
export const CHANGE_SOURCE_PATH = '/changes/v0/stream';
