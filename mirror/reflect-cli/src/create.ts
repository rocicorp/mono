import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import color from 'picocolors';
import {scaffoldHandler, updateEnvFile} from './scaffold.js';
import {getApp, initHandler} from './init.js';
import {publishHandler} from './publish.js';
import {readAppConfig} from './app-config.js';
import {getFirestore} from './firebase.js';

export function createOptions(yargs: CommonYargsArgv) {
  return yargs.option('name', {
    describe: 'Name of the app',
    type: 'string',
    demandOption: true,
  });
}

type CreatedHandlerArgs = YargvToInterface<ReturnType<typeof createOptions>>;

export async function createHandler(createYargs: CreatedHandlerArgs) {
  const {name} = createYargs;
  scaffoldHandler(createYargs);
  await initHandler(
    {
      ...createYargs,
      channel: 'stable',
      new: true,
    },
    name,
  );
  await publishHandler(
    {
      ...createYargs,
      script: `${name}/src/reflect/index.ts`,
    },
    name,
  );
  const firestore = getFirestore();
  const appConfig = readAppConfig(name);
  if (appConfig) {
    const app = await getApp(firestore, appConfig.appID);
    console.log(
      `Updating app .env with worker wss://${app.name}.reflect-server.net`,
    );
    updateEnvFile(name, `wss://${app.name}.reflect-server.net`);
  }
  console.log(color.blue(`start-up your reflect app:`));
  console.log(color.white(`cd ${name} && npm install && npm run dev`));
}
