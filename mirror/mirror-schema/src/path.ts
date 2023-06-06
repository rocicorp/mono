type FirestorePathSegment = string;
type FirestorePath = string;

const MAX_FIRESTORE_ID_LENGTH = 1500;

/**
 * Assembles a Firestore Path string by joining `segments` with a `/` character.
 *
 * The segments are checked for invalid characters
 * (https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields)
 * and an `InvalidPathSegmentError` is thrown if one is found.
 *
 * Examples:
 * ```
 * join('users', 'foo') // 'users/foo'
 *
 * const fooPath = join('users', 'foo')
 * join(fooPath, 'subcollection') // 'users/foo/subcollection'
 *
 * join('users', 'foo/subcollection') // throws error
 *
 * join('users/foo', 'subcollection') // 'users/foo/subcollection'
 * ```
 */
export function join(...segments: FirestorePathSegment[]): FirestorePath {
  return assemble(null, segments);
}

/**
 * Appends to a Firestore Path `basePath` string by joining additional
 * `segments` with a `/` character.
 *
 * The segments are checked for invalid characters
 * (https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields)
 * and an
 * `InvalidPathSegmentError` is thrown if one is found. The `basePath` is
 * unchecked.
 *
 * Examples:
 * ```
 * append('users', 'foo') // 'users/foo'
 *
 * const fooPath = append('users', 'foo')
 * append(fooPath, 'subcollection') // 'users/foo/subcollection'
 *
 * append('users', 'foo/subcollection') // throws error
 * ```
 */
export function append(
  basePath: FirestorePath,
  ...segments: FirestorePathSegment[]
): FirestorePath {
  return assemble(basePath, segments);
}

/**
 * Superclass of Errors thrown for inputs that would result in
 * a path that violates Firestore rules.
 */
export class InvalidPathError extends Error {
  constructor(reason: string, assembledPath: FirestorePath) {
    super(`${reason}: ${assembledPath}`);
  }
}

export class InvalidPathSegmentError extends InvalidPathError {
  constructor(arg: FirestorePathSegment, assembledPath: FirestorePath) {
    super(`Invalid path segment received: "${arg}"`, assembledPath);
  }
}

export class InvalidPathLengthError extends InvalidPathError {
  constructor(assembledPath: FirestorePath) {
    super('Path is too long', assembledPath);
  }
}

function assemble(
  prefix: string | null,
  segments: FirestorePathSegment[],
): FirestorePath {
  const assembled =
    prefix === null ? segments.join('/') : [prefix, ...segments].join('/');
  segments.forEach(segment => {
    if (!segment || segment.indexOf('/') !== -1) {
      throw new InvalidPathSegmentError(segment, assembled);
    }
    if (segment === '.' || segment === '..' || /__.*__/.test(segment)) {
      throw new InvalidPathSegmentError(segment, assembled);
    }
  });
  if (assembled.length > MAX_FIRESTORE_ID_LENGTH) {
    throw new InvalidPathLengthError(assembled);
  }
  return assembled;
}
