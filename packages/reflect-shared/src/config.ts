const defaultConfig = {
  closeBeacon: true,
};
Object.freeze(defaultConfig);

type Config = typeof defaultConfig;

type ConfigNames = keyof Config;

let config: Config = {...defaultConfig};

export function setConfig<Name extends ConfigNames>(
  name: Name,
  value: Config[Name],
): void {
  if (Object.hasOwn(config, name)) {
    config[name] = value;
    return;
  }
  throw new Error(`Unknown config: ${name}`);
}

export function resetConfig<Name extends ConfigNames>(name: Name): void {
  setConfig(name, defaultConfig[name]);
}

export function resetAllConfig(): void {
  config = {...defaultConfig};
}

export function getConfig<Name extends ConfigNames>(name: Name): Config[Name] {
  return config[name];
}
