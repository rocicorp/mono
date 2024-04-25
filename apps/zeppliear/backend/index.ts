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
        for (const member of sampleData.members) {
          await mutators.putMember(write, {
            member,
          });
        }
        for (const issue of sampleData.issues) {
          await mutators.putIssue(write, {
            issue,
          });
        }
        for (const comment of sampleData.comments) {
          await mutators.putIssueComment(write, {
            comment,
            updateIssueModifed: false,
          });
        }
        for (const label of sampleData.labels) {
          await mutators.putLabel(write, {
            label,
          });
        }
        for (const issueLabel of sampleData.issueLabels) {
          await mutators.putIssueLabel(write, {
            link: issueLabel,
          });
        }
        await write.set('inited', true);
      }
    },
  };
}

export {makeOptions as default};
