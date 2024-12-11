const prefix = 'comment-';

export function parsePermalink(hash: string): string | undefined {
  return hash.startsWith(prefix) ? hash.slice(prefix.length) : undefined;
}

export function makePermalink(comment: {id: string}): string {
  return prefix + comment.id;
}
