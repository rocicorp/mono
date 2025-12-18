/* oxlint-disable */
/// <reference path="./.sst/platform/config.d.ts" />
// Load .env file
require('@dotenvx/dotenvx').config();
import {createDefu} from 'defu';
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
      ZERO_APP_PUBLICATIONS: process.env.ZERO_APP_PUBLICATIONS!,
      ZERO_UPSTREAM_DB: process.env.ZERO_UPSTREAM_DB!,
      ZERO_ADMIN_PASSWORD: process.env.ZERO_ADMIN_PASSWORD!,
      ZERO_MUTATE_URL: process.env.ZERO_MUTATE_URL!,
      ZERO_QUERY_URL: process.env.ZERO_QUERY_URL!,
      ZERO_CVR_DB: process.env.ZERO_CVR_DB!,
      ZERO_CHANGE_DB: process.env.ZERO_CHANGE_DB!,
      ZERO_AUTH_JWK: process.env.ZERO_AUTH_JWK!,
      ZERO_LOG_FORMAT: 'json',
      ZERO_ENABLE_QUERY_PLANNER:
        process.env.ZERO_ENABLE_QUERY_PLANNER || 'true',
      ZERO_REPLICA_FILE: IS_EBS_STAGE
        ? '/data/sync-replica.db'
        : 'sync-replica.db',
      ZERO_IMAGE_URL: process.env.ZERO_IMAGE_URL!,
      ZERO_APP_ID: process.env.ZERO_APP_ID || 'zero',
      PGCONNECT_TIMEOUT: '60', // scale-from-zero dbs need more than 30 seconds
      OTEL_TRACES_EXPORTER: 'otlp',
      OTEL_LOGS_EXPORTER: 'none',
      OTEL_EXPORTER_OTLP_ENDPOINT: process.env.OTEL_EXPORTER_OTLP_ENDPOINT,
      OTEL_EXPORTER_OTLP_HEADERS: process.env.OTEL_EXPORTER_OTLP_HEADERS,
      OTEL_RESOURCE_ATTRIBUTES: process.env.OTEL_RESOURCE_ATTRIBUTES,
      OTEL_NODE_ENABLED_INSTRUMENTATIONS:
        '@opentelemetry/instrumentation-runtime-node',
      OTEL_NODE_RESOURCE_DETECTORS: 'env,host,os',
      OTEL_LOG_LEVEL: 'warn',
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
                volumeType: 'gp3',
                // Note: The maximum allowed IOPS/GB ratio is 500.
                sizeInGb: process.env.EBS_SIZE_GB || 30,
                iops: process.env.EBS_IOPS || 15000,
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

    const replicationManager = new sst.aws.Service(`replication-manager`, {
      cluster,
      cpu: IS_EBS_STAGE ? '16 vCPU' : '2 vCPU',
      memory: IS_EBS_STAGE ? '32 GB' : '8 GB',
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
        ZERO_LOG_LEVEL: 'debug',
        ZERO_LITESTREAM_BACKUP_URL: $interpolate`s3://${replicationBucket.name}/backup/20251218-02`,
        ZERO_INITIAL_SYNC_PROFILE_COPY: 'true',
        ZERO_CHANGE_MAX_CONNS: '3',
        ZERO_NUM_SYNC_WORKERS: '0',
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
      logging: {
        retention: '1 month',
      },
      transform: defu(EBS_TRANSFORM, BASE_TRANSFORM),
    });

    // View Syncer Service
    const viewSyncer = new sst.aws.Service(`view-syncer`, {
      cluster,
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
  },
});
