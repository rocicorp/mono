/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />


export default $config({
  app(input) {
    return {
      name: process.env.APP_NAME || 'zero',
      removal: input?.stage === 'production' ? 'retain' : 'remove',
      home: 'aws',
      region: process.env.AWS_REGION || 'us-east-1',
      providers: {command: '1.0.2', docker: true},
    };
  },
  async run() {
    const {createDefu} = await import('defu');
    const { networkConfig } = await import( './infra/network');
    const { createAlb } = await import( './infra/alb');
    const {join} = await import('node:path');

    const defu = createDefu((obj, key, value) => {
      // Don't merge functions, just use the last one
      if (typeof obj[key] === 'function' || typeof value === 'function') {
        obj[key] = value;
        return true;
      }
      return false;
    });

    const replicationBucket = new sst.aws.Bucket(`replication-bucket`, {
      public: false,
    });

    const network = networkConfig(`${$app.name}-${$app.stage}`);

    const alb = createAlb(`${$app.name}-${$app.stage}`, {
      domainName: process.env.DOMAIN_NAME,
      domainCertArn: process.env.DOMAIN_CERT,
      vpcId: network.vpcId,
      publicSubnets: network.albPublicSunets,
      privateSubnets: network.privateSubnets,
    });

    // Common environment variables
    const commonEnv = {
      AWS_REGION: process.env.AWS_REGION!,
      ZERO_UPSTREAM_DB: process.env.ZERO_UPSTREAM_DB!,
      ZERO_PUSH_URL: process.env.ZERO_PUSH_URL!,
      ZERO_CVR_DB: process.env.ZERO_CVR_DB!,
      ZERO_CHANGE_DB: process.env.ZERO_CHANGE_DB!,
      ZERO_AUTH_JWK: process.env.ZERO_AUTH_JWK!,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID!,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY!,
      ZERO_LOG_FORMAT: 'json',
      ZERO_REPLICA_FILE: 'sync-replica.db',
      ZERO_LITESTREAM_BACKUP_URL: $interpolate`s3://${replicationBucket.name}/backup/20250319-00`,
      ZERO_IMAGE_URL: process.env.ZERO_IMAGE_URL!,
      ZERO_APP_ID: process.env.ZERO_APP_ID || 'zero',
    };





    new command.local.Command('zero-deploy-permissions', {
      // Pulumi operates with cwd at the package root.
      dir: join(process.cwd(), '../../packages/zero/'),
      create: `npx zero-deploy-permissions --schema-path ../../apps/zbugs/shared/schema.ts`,
      environment: {
        ['ZERO_UPSTREAM_DB']: process.env.ZERO_UPSTREAM_DB,
        ['ZERO_APP_ID']: process.env.ZERO_APP_ID,
      },
      // Run the Command on every deploy.
      triggers: [Date.now()],
    });
  },
});
