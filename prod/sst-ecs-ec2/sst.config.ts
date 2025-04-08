/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />

// Define type for environment variables
interface ZeroEnvironmentVars {
  AWS_REGION: string;
  ZERO_UPSTREAM_DB: string;
  ZERO_PUSH_URL: string;
  ZERO_CVR_DB: string;
  ZERO_CHANGE_DB: string;
  ZERO_AUTH_JWK: string;
  AWS_ACCESS_KEY_ID: string;
  AWS_SECRET_ACCESS_KEY: string;
  ZERO_LOG_FORMAT: string;
  ZERO_REPLICA_FILE: string;
  ZERO_LITESTREAM_BACKUP_URL: string | $util.Output<string>;
  ZERO_IMAGE_URL: string;
  ZERO_APP_ID: string;
  ZERO_COMMAND?: string; // Optional for service-specific command
  [key: string]: string | $util.Output<string> | undefined; // Allow for any additional environment variables
}

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
    const { capacityProvider } = await import( './infra/capacity-provider');
    const { createService } = await import( './infra/service');


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

    const cluster = new aws.ecs.Cluster(`${$app.name}-${$app.stage}-cluster`, {
      name: `${$app.name}-${$app.stage}-cluster`,
      settings: [
        {
          name: "containerInsights",
          value: "enabled"
        }
      ]
    });

    const provider = await capacityProvider(`${$app.name}-${$app.stage}`,{
      vpcId: network.vpcId,
      privateSubnets: network.privateSubnets,
      cluster,
    });

    // Common environment variables with proper typing
    const commonEnv: ZeroEnvironmentVars = {
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
      ZERO_LITESTREAM_BACKUP_URL: replicationBucket.name.apply(name => 
        `s3://${name}/backup/20250319-00`
      ),
      ZERO_IMAGE_URL: process.env.ZERO_IMAGE_URL!,
      ZERO_APP_ID: process.env.ZERO_APP_ID || 'zero',
    };

    // Create the view-syncer service - ensure it depends on the capacity provider
    const viewSyncerService = createService(`${$app.name}-${$app.stage}-view-syncer`, {
      vpcId: network.vpcId,
      privateSubnets: network.privateSubnets,
      cluster,
      capacityProvider: provider.capacityProvider,
      targetGroup: alb.targetGroup,
      albSecurityGroup: alb.albSecurityGroup,
      commonEnv: {
        ...commonEnv,
        ZERO_COMMAND: 'view-syncer',
      },
      port: 4848, // Default port in service.ts is already 4848
    });

    // Create the replication manager service - ensure it depends on the capacity provider
    const replicationManagerService = createService(`${$app.name}-${$app.stage}-replication-manager`, {
      vpcId: network.vpcId,
      privateSubnets: network.privateSubnets,
      cluster,
      capacityProvider: provider.capacityProvider,
      targetGroup: alb.internalTargetGroup,
      albSecurityGroup: alb.internalAlbSecurityGroup || alb.albSecurityGroup,
      commonEnv: {
        ...commonEnv,
        ZERO_COMMAND: 'replication-manager',
      },
      port: 4849, // Port for replication-manager
    });

 

    // new command.local.Command('zero-deploy-permissions', {
    //   // Pulumi operates with cwd at the package root.
    //   dir: join(process.cwd(), '../../packages/zero/'),
    //   create: `npx zero-deploy-permissions --schema-path ../../apps/zbugs/shared/schema.ts`,
    //   environment: {
    //     ['ZERO_UPSTREAM_DB']: process.env.ZERO_UPSTREAM_DB,
    //     ['ZERO_APP_ID']: process.env.ZERO_APP_ID,
    //   },
    //   // Run the Command on every deploy.
    //   triggers: [Date.now()],
    // });
  },
});
