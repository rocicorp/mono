import {Timestamp} from '@google-cloud/firestore';
import {describe, expect, test} from '@jest/globals';
import {parse} from 'shared/src/valita.js';
import {appSchema} from './app.js';
import {defaultOptions, deploymentSchema, varsSchema} from './deployment.js';

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
    const parsedApp = appSchema.parse({
      cfID: 'foo',
      cfScriptName: 'bar',
      name: 'baz',
      serverReleaseChannel: 'stable',
      teamID: 'boo',
      teamLabel: 'teamlabel',
      deploymentOptions: {vars: {}},
      secrets: {},
    });

    const parsedDeployment = deploymentSchema.parse(
      {
        deploymentID: 'boo',
        requesterID: 'foo',
        type: 'USER_UPLOAD',
        spec: {
          appModules: [],
          hostname: 'bar',
          serverVersionRange: 'baz',
          serverVersion: 'boo',
          options: {vars: {DISABLE_LOG_FILTERING: 'false'}},
          hashesOfSecrets: {
            REFLECT_AUTH_API_KEY: 'aaa',
            DATADOG_LOGS_API_KEY: 'bbb',
            DATADOG_METRICS_API_KEY: 'ccc',
          },
        },
        requestTime: Timestamp.now(),
        status: 'REQUESTED',
      },
      {mode: 'passthrough'},
    );

    expect(parsedApp.deploymentOptions?.vars).toEqual(
      parsedDeployment.spec.options?.vars,
    );
  });
  /* eslint-enable @typescript-eslint/naming-convention */
});
