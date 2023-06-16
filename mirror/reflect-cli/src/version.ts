import packageJSON from '../package.json' assert {type: 'json'};

export const {version} = packageJSON;

export const userAgent = {
  type: packageJSON.name,
  version,
} as const;

export type UserAgent = typeof userAgent;
