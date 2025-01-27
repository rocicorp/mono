export function avatarURLWithSize(avatarURL: string): string;
export function avatarURLWithSize(avatarURL: undefined): undefined;
export function avatarURLWithSize(
  avatarURL: string | undefined,
): string | undefined;
export function avatarURLWithSize(
  avatarURL: string | undefined,
): string | undefined {
  if (!avatarURL) {
    return undefined;
  }
  try {
    // For github avatar URLs we add `?size=64`. See https://bugs.rocicorp.dev/issue/3270
    // https://avatars.githubusercontent.com/u/45845?v=4
    const url = new URL(avatarURL);
    if (url.host === 'avatars.githubusercontent.com') {
      url.searchParams.set('size', '64');
      return url.toString();
    }
  } catch {
    // Ignore errors
  }
  return avatarURL;
}
