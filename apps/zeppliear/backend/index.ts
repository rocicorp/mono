import type {ReflectServerOptions} from '@rocicorp/reflect/server';
import {mutators, M} from '../frontend/mutators';
import {getReactSampleData} from './sample-issues';

function makeOptions(): ReflectServerOptions<M> {
  return {
    mutators,
    roomStartHandler: async write => {
      const inited = await write.get('inited');
      if (inited !== true) {
        const sampleData = getReactSampleData();
        for (const {issue, description, comments} of sampleData) {
          await mutators.putIssue(write, {
            issue,
            description: description.substring(0, 10000),
          });
          for (const comment of comments) {
            await mutators.putIssueComment(write, comment);
          }
        }
        await write.set('inited', true);
      }
    },
  };
}

export {makeOptions as default};
