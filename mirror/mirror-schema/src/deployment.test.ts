import {describe, expect, test} from '@jest/globals';
import {parse} from 'shared/src/valita.js';
import {defaultOptions, varsSchema} from './deployment.js';
import {envSchema} from './env.js';

describe('deployment', () => {
  /* eslint-disable @typescript-eslint/naming-convention */
  test('default vars', () => {
    expect(varsSchema.parse({})).toEqual({
      DISABLE: 'false',
      DISABLE_LOG_FILTERING: 'false',
      LOG_LEVEL: 'info',
    });

    expect(parse({}, varsSchema)).toEqual({
      DISABLE: 'false',
      DISABLE_LOG_FILTERING: 'false',
      LOG_LEVEL: 'info',
    });

    expect(parse({LOG_LEVEL: 'debug'}, varsSchema)).toEqual({
      DISABLE: 'false',
      DISABLE_LOG_FILTERING: 'false',
      LOG_LEVEL: 'debug',
    });
  });

  test('normalize vars', () => {
    const vars = {
      DISABLE_LOG_FILTERING: 'true',
    };

    expect(parse(vars, varsSchema)).toEqual({
      DISABLE: 'false',
      DISABLE_LOG_FILTERING: 'true',
      LOG_LEVEL: 'info',
    });

    expect(vars).toEqual({
      DISABLE_LOG_FILTERING: 'true',
    });
  });

  test('defaultOptions', () => {
    expect(defaultOptions()).toEqual({
      vars: {
        DISABLE: 'false',
        DISABLE_LOG_FILTERING: 'false',
        LOG_LEVEL: 'info',
      },
    });
  });

  test('protoless valita defaultOptions', () => {
    const protoless = Object.freeze(Object.create(null));
    const defaultDeploy = Object.create(protoless);
    defaultDeploy['DISABLE_LOG_FILTERING'] = 'false';
    defaultDeploy['LOG_LEVEL'] = 'info';
    expect(varsSchema.parse({}).constructor).toBeDefined();
    expect(
      defaultDeploy.constructor === varsSchema.parse({}).constructor,
    ).toEqual(false);
    expect(defaultOptions().vars).toEqual(varsSchema.parse({}));
  });

  test('vars embedded in docs are normalized on parse', () => {
    const parsedEnv = envSchema.parse(
      {
        deploymentOptions: {vars: {}},
        secrets: {},
      },
      {mode: 'passthrough'},
    );

    expect(parsedEnv).toEqual({
      deploymentOptions: {
        vars: {
          DISABLE: 'false',
          DISABLE_LOG_FILTERING: 'false',
          LOG_LEVEL: 'info',
        },
      },
      secrets: {},
    });
  });
  /* eslint-enable @typescript-eslint/naming-convention */
});
