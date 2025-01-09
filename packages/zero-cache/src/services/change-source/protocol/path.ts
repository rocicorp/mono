// TODO: Repackage and export this in a `change-protocol` package.

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
