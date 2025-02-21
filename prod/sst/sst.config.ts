/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />
// Load .env file
require("dotenv").config();

import { join } from "node:path";

export default $config({
  app(input) {
    return {
      name: process.env.APP_NAME || "zero",
      removal: input?.stage === "production" ? "retain" : "remove",
      home: "aws",
      region: process.env.AWS_REGION || "us-east-1",
      providers: { command: "1.0.2" },
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
      nat: "ec2", // Needed for deploying Lambdas
    });
    // ECS Cluster
    const cluster = new sst.aws.Cluster(`cluster`, {
      vpc,
      transform: {
        cluster: {
          settings: [
            {
              name: "containerInsights",
              value: "enhanced",
            },
          ],
        },
      },
    });
    // Common environment variables
    const commonEnv = {
      AWS_REGION: process.env.AWS_REGION!,
      ZERO_UPSTREAM_DB: process.env.ZERO_UPSTREAM_DB!,
      ZERO_CVR_DB: process.env.ZERO_CVR_DB!,
      ZERO_CHANGE_DB: process.env.ZERO_CHANGE_DB!,
      ZERO_AUTH_SECRET: process.env.ZERO_AUTH_SECRET!,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID!,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY!,
      ZERO_LOG_FORMAT: "json",
      ZERO_REPLICA_FILE: "sync-replica.db",
      ZERO_LITESTREAM_BACKUP_URL: $interpolate`s3://${replicationBucket.name}/backup/20250211-00`,
      ZERO_IMAGE_URL: process.env.ZERO_IMAGE_URL!,
    };
    // Replication Manager Service
    const replicationManager = cluster.addService(`replication-manager`, {
      cpu: "2 vCPU",
      memory: "8 GB",
      image: commonEnv.ZERO_IMAGE_URL,
      health: {
        command: ["CMD-SHELL", "curl -f http://localhost:4849/ || exit 1"],
        interval: "5 seconds",
        retries: 3,
        startPeriod: "300 seconds",
      },
      environment: {
        ...commonEnv,
        ZERO_CHANGE_MAX_CONNS: "3",
        ZERO_NUM_SYNC_WORKERS: "0",
      },
      logging: {
        retention: "1 month",
      },
      loadBalancer: {
        public: false,
        ports: [
          {
            listen: "80/http",
            forward: "4849/http",
          },
        ],
      },
      transform: {
        service: {
          healthCheckGracePeriodSeconds: 300, // same as health.startPeriod
        },
        loadBalancer: {
          idleTimeout: 3600, // Keep idle connections alive
        },
        target: {
          healthCheck: {
            enabled: true,
            path: "/keepalive",
            protocol: "HTTP",
            interval: 5,
            healthyThreshold: 2,
            timeout: 3,
          },
          deregistrationDelay: 1, // Drain as soon as a new instance is healthy.
        },
      },
    });
    // View Syncer Service
    const viewSyncer = cluster.addService(`view-syncer`, {
      cpu: "8 vCPU",
      memory: "16 GB",
      image: commonEnv.ZERO_IMAGE_URL,
      health: {
        command: ["CMD-SHELL", "curl -f http://localhost:4848/ || exit 1"],
        interval: "5 seconds",
        retries: 3,
        startPeriod: "300 seconds",
      },
      environment: {
        ...commonEnv,
        ZERO_CHANGE_STREAMER_URI: replicationManager.url,
        ZERO_UPSTREAM_MAX_CONNS: "15",
        ZERO_CVR_MAX_CONNS: "160",
      },
      logging: {
        retention: "1 month",
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
                  listen: "80/http",
                  forward: "4848/http",
                },
                {
                  listen: "443/https",
                  forward: "4848/http",
                },
              ],
            }
          : {
              ports: [
                {
                  listen: "80/http",
                  forward: "4848/http",
                },
              ],
            }),
      },
      transform: {
        service: {
          healthCheckGracePeriodSeconds: 300, // same as health.startPeriod
        },
        target: {
          healthCheck: {
            enabled: true,
            path: "/keepalive",
            protocol: "HTTP",
            interval: 5,
            healthyThreshold: 2,
            timeout: 3,
          },
          stickiness: {
            enabled: true,
            type: "lb_cookie",
            cookieDuration: 120,
          },
          loadBalancingAlgorithmType: "least_outstanding_requests",
        },
        autoScalingTarget: {
          minCapacity: 1,
          maxCapacity: 10,
        },
      },
      // Set this to `true` to make SST wait for the view-syncer to be deployed
      // before proceeding (to permissions deployment, etc.). This makes the deployment
      // take a lot longer and is only necessary if there is an AST format change.
      wait: false,
    });

    if ($app.stage === "sandbox") {
      // In sandbox, deploy permissions in a Lambda.
      const permissionsDeployer = new sst.aws.Function(
        "zero-permissions-deployer",
        {
          handler: "../functions/src/permissions.deploy",
          vpc,
          environment: { ["ZERO_UPSTREAM_DB"]: process.env.ZERO_UPSTREAM_DB },
          copyFiles: [
            { from: "../../apps/zbugs/schema.ts", to: "./schema.ts" },
          ],
          nodejs: { install: ["@rocicorp/zero"] },
        },
      );

      new aws.lambda.Invocation(
        "invoke-zero-permissions-deployer",
        {
          // Invoke the Lambda on every deploy.
          input: Date.now().toString(),
          functionName: permissionsDeployer.name,
        },
        { dependsOn: viewSyncer },
      );
    } else {
      // In prod, deploy permissions via a local Command, to exercise both approaches.
      new command.local.Command(
        "zero-deploy-permissions",
        {
          // Pulumi operates with cwd at the package root.
          dir: join(process.cwd(), "../../packages/zero/"),
          create: `npx zero-deploy-permissions --schema-path ../../apps/zbugs/schema.ts`,
          environment: { ["ZERO_UPSTREAM_DB"]: process.env.ZERO_UPSTREAM_DB },
          // Run the Command on every deploy.
          triggers: [Date.now()],
        },
        // after the view-syncer is deployed.
        { dependsOn: viewSyncer },
      );
    }
  },
});
