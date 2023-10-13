export type ScriptTags = {
  appID: string;
  appName: string;
  teamID: string;
  teamLabel: string;
};

export function parseScriptTags(scriptTags: string[]): ScriptTags {
  const map = new Map<string, string>();
  scriptTags.forEach(tag => {
    const colon = tag.indexOf(':');
    if (colon > 0) {
      map.set(tag.substring(0, colon), tag.substring(colon + 1));
    }
  });
  function getValue(key: string): string {
    const val = map.get(key);
    if (!val) {
      throw new TypeError(`Missing scriptTag for ${key}: ${scriptTags}`);
    }
    return val;
  }
  return {
    appID: getValue('appID'),
    appName: getValue('appName'),
    teamID: getValue('teamID'),
    teamLabel: getValue('teamLabel'),
  };
}
