/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />
// Load .env file
require('@dotenvx/dotenvx').config();

import {createDefu} from 'defu';
import {join} from 'node:path';

const defu = createDefu((obj, key, value) => {
  // Don't merge functions, just use the last one
  if (typeof obj[key] === 'function' || typeof value === 'function') {
    obj[key] = value;
    return true;
  }
  return false;
});

export default $config({
  app(input) {
    return {
      name: process.env.APP_NAME || 'zero',
      removal: input?.stage === 'production' ? 'retain' : 'remove',
      home: 'aws',
      region: process.env.AWS_REGION || 'us-east-1',
      providers: {command: '1.0.2'},
    };
  },
  async run() {
    // S3 Bucket
    const replicationBucket = new sst.aws.Bucket(`replication-bucket`, {
      public: false,
    });
    // VPC Configuration
    const vpc = new sst.aws.Vpc(`vpc`, {
      az: 2,
      nat: 'ec2', // Needed for deploying Lambdas
    });
    // ECS Cluster
    const cluster = new sst.aws.Cluster(`cluster`, {
      vpc,
      transform: {
        cluster: {
          settings: [
            {
              name: 'containerInsights',
              value: 'enhanced',
            },
          ],
        },
      },
    });

    const IS_EBS_STAGE = $app.stage.endsWith('-ebs');

    // Common environment variables
    const commonEnv = {
      ZERO_UPSTREAM_DB: process.env.ZERO_UPSTREAM_DB!,
      ZERO_PUSH_URL: process.env.ZERO_PUSH_URL!,
      ZERO_CVR_DB: process.env.ZERO_CVR_DB!,
      ZERO_CHANGE_DB: process.env.ZERO_CHANGE_DB!,
      ZERO_AUTH_JWK: process.env.ZERO_AUTH_JWK!,
      ZERO_LOG_FORMAT: 'json',
      ZERO_REPLICA_FILE: IS_EBS_STAGE
        ? '/data/sync-replica.db'
        : 'sync-replica.db',
      ZERO_LITESTREAM_BACKUP_URL: $interpolate`s3://${replicationBucket.name}/backup/20250319-00`,
      ZERO_IMAGE_URL: process.env.ZERO_IMAGE_URL!,
      ZERO_APP_ID: process.env.ZERO_APP_ID || 'zero',
    };

    const ecsVolumeRole = IS_EBS_STAGE
      ? new aws.iam.Role(`${$app.name}-${$app.stage}-ECSVolumeRole`, {
          assumeRolePolicy: JSON.stringify({
            Version: '2012-10-17',
            Statement: [
              {
                Effect: 'Allow',
                Principal: {
                  Service: ['ecs-tasks.amazonaws.com', 'ecs.amazonaws.com'],
                },
                Action: 'sts:AssumeRole',
              },
            ],
          }),
        })
      : undefined;

    if (ecsVolumeRole) {
      new aws.iam.RolePolicyAttachment(
        `${$app.name}-${$app.stage}-ECSVolumePolicyAttachment`,
        {
          role: ecsVolumeRole.name,
          policyArn:
            'arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRolePolicyForVolumes',
        },
      );
    }

    // Common base transform configuration
    const BASE_TRANSFORM: any = {
      service: {
        // 10 minutes should be more than enough time for gigabugs initial-sync.
        healthCheckGracePeriodSeconds: 600,
      },
      loadBalancer: {
        idleTimeout: 3600,
      },
      target: {
        healthCheck: {
          enabled: true,
          path: '/keepalive',
          protocol: 'HTTP',
          interval: 5,
          healthyThreshold: 2,
          timeout: 3,
        },
        deregistrationDelay: 1,
      },
    };

    // EBS-specific transform configuration
    const EBS_TRANSFORM: any = !IS_EBS_STAGE
      ? {}
      : {
          service: {
            volumeConfiguration: {
              name: 'replication-data',
              managedEbsVolume: {
                roleArn: ecsVolumeRole?.arn,
                volumeType: 'io2',
                sizeInGb: 20,
                iops: 3000,
                fileSystemType: 'ext4',
              },
            },
          },
          taskDefinition: (args: any) => {
            let value = $jsonParse(args.containerDefinitions);
            value = value.apply((containerDefinitions: any) => {
              containerDefinitions[0].mountPoints = [
                {
                  sourceVolume: 'replication-data',
                  containerPath: '/data',
                },
              ];
              return containerDefinitions;
            });
            args.containerDefinitions = $jsonStringify(value);
            args.volumes = [
              {
                name: 'replication-data',
                configureAtLaunch: true,
              },
            ];
          },
        };

    // Replication Manager Service
    const replicationManager = cluster.addService(`replication-manager`, {
      cpu: '2 vCPU',
      memory: '8 GB',
      image: commonEnv.ZERO_IMAGE_URL,
      link: [replicationBucket],
      health: {
        command: ['CMD-SHELL', 'curl -f http://localhost:4849/ || exit 1'],
        interval: '5 seconds',
        retries: 3,
        startPeriod: '300 seconds',
      },
      environment: {
        ...commonEnv,
        ZERO_CHANGE_MAX_CONNS: '3',
        ZERO_NUM_SYNC_WORKERS: '0',
      },
      logging: {
        retention: '1 month',
      },
      loadBalancer: {
        public: false,
        ports: [
          {
            listen: '80/http',
            forward: '4849/http',
          },
        ],
      },
      transform: defu(EBS_TRANSFORM, BASE_TRANSFORM),
    });
    // View Syncer Service
    const viewSyncer = cluster.addService(`view-syncer`, {
      cpu: '8 vCPU',
      memory: '16 GB',
      image: commonEnv.ZERO_IMAGE_URL,
      link: [replicationBucket],
      health: {
        command: ['CMD-SHELL', 'curl -f http://localhost:4848/ || exit 1'],
        interval: '5 seconds',
        retries: 3,
        startPeriod: '300 seconds',
      },
      environment: {
        ...commonEnv,
        ZERO_CHANGE_STREAMER_URI: replicationManager.url,
        ZERO_UPSTREAM_MAX_CONNS: '15',
        ZERO_CVR_MAX_CONNS: '160',
      },
      logging: {
        retention: '1 month',
      },
      loadBalancer: {
        public: true,
        //only set domain if both are provided
        ...(process.env.DOMAIN_NAME && process.env.DOMAIN_CERT
          ? {
              domain: {
                name: process.env.DOMAIN_NAME,
                dns: false,
                cert: process.env.DOMAIN_CERT,
              },
              ports: [
                {
                  listen: '80/http',
                  forward: '4848/http',
                },
                {
                  listen: '443/https',
                  forward: '4848/http',
                },
              ],
            }
          : {
              ports: [
                {
                  listen: '80/http',
                  forward: '4848/http',
                },
              ],
            }),
      },
      transform: defu(EBS_TRANSFORM, BASE_TRANSFORM, {
        target: {
          stickiness: {
            enabled: true,
            type: 'lb_cookie',
            cookieDuration: 120,
          },
          loadBalancingAlgorithmType: 'least_outstanding_requests',
        },
        autoScalingTarget: {
          minCapacity: 1,
          maxCapacity: 10,
        },
      }),
      // Set this to `true` to make SST wait for the view-syncer to be deployed
      // before proceeding (to permissions deployment, etc.). This makes the deployment
      // take a lot longer and is only necessary if there is an AST format change.
      wait: false,
    });

    if ($app.stage === 'sandbox') {
      // In sandbox, deploy permissions in a Lambda.
      const permissionsDeployer = new sst.aws.Function(
        'zero-permissions-deployer',
        {
          handler: '../functions/src/permissions.deploy',
          vpc,
          environment: {
            ['ZERO_UPSTREAM_DB']: process.env.ZERO_UPSTREAM_DB,
            ['ZERO_APP_ID']: process.env.ZERO_APP_ID,
          },
          copyFiles: [
            {from: '../../apps/zbugs/shared/schema.ts', to: './schema.ts'},
          ],
          nodejs: {install: ['@rocicorp/zero']},
        },
      );

      new aws.lambda.Invocation(
        'invoke-zero-permissions-deployer',
        {
          // Invoke the Lambda on every deploy.
          input: Date.now().toString(),
          functionName: permissionsDeployer.name,
        },
        {dependsOn: viewSyncer},
      );
    } else {
      // In prod, deploy permissions via a local Command, to exercise both approaches.
      new command.local.Command(
        'zero-deploy-permissions',
        {
          // Pulumi operates with cwd at the package root.
          dir: join(process.cwd(), '../../packages/zero/'),
          create: `npx zero-deploy-permissions --schema-path ../../apps/zbugs/shared/schema.ts`,
          environment: {
            ['ZERO_UPSTREAM_DB']: process.env.ZERO_UPSTREAM_DB,
            ['ZERO_APP_ID']: process.env.ZERO_APP_ID,
          },
          // Run the Command on every deploy.
          triggers: [Date.now()],
        },
        // after the view-syncer is deployed.
        {dependsOn: viewSyncer},
      );
    }
  },
});
