export function createPayload(
  subscriber: number,
  message: number,
  payloadBytes: number,
) {
  const base = {
    tag: 'insert',
    relation: {schema: 'public', name: 'issues', replicaIdentity: 'default'},
    new: {
      id: `issue-${subscriber}-${message}`,
      title: 'the view-syncer is behind',
      owner: 'rm',
      body: '',
    },
  };
  const withoutBody = JSON.stringify(['data', base]);
  base.new.body = 'x'.repeat(Math.max(0, payloadBytes - withoutBody.length));
  return JSON.stringify(['data', base]);
}
