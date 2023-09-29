/**
 * Names of npm `dist-tags` used for version management.
 */
export const enum DistTag {
  Latest = 'latest',
  MinSupported = 'sup',
  MinNonDeprecated = 'rec',
}

export type DistTagMap<SemVer> = {[tag: string]: SemVer};

// SemVer is left as a type parameter (instead of directly referenced) to avoid
// incompatibilities across libraries (e.g. mirror-protocol vs reflect-cli) due to
// different instances of the SemVer library not recognizing each other's classes.
// Callers instead pass in their own SemVer constructor, e.g.
//
// ```
// const distTags = await lookupDistTags(SemVer, 5000);
// ```
export async function lookupDistTags<SemVer>(
  semVerCtor: new (val: string) => SemVer,
  timeout: number,
): Promise<DistTagMap<SemVer>> {
  // https://github.com/npm/registry/blob/master/docs/REGISTRY-API.md
  const resp = await fetch('https://registry.npmjs.org/@rocicorp/reflect', {
    signal: AbortSignal.timeout(timeout),
  });
  const pkgMeta = await resp.json();
  const distTags = pkgMeta['dist-tags'] as Record<string, string>;
  return Object.fromEntries(
    Object.entries(distTags).map(([tag, value]) => [
      tag,
      new semVerCtor(value),
    ]),
  );
}
