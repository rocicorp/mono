/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />
import { readFileSync } from "fs";
// Load .env file
require("dotenv").config();

export default $config({
  app(input) {
    return {
      name: process.env.APP_NAME || "zero",
      removal: input?.stage === "production" ? "retain" : "remove",
      home: "aws",
      region: process.env.AWS_REGION || "us-east-1",
    };
  },
  async run() {
    const loadSchemaJson = () => {
      if (process.env.ZERO_SCHEMA_JSON) {
        return process.env.ZERO_SCHEMA_JSON;
      }

      try {
        const schema = readFileSync("zero-schema.json", "utf8");
        // Parse and stringify to ensure single line
        return JSON.stringify(JSON.parse(schema));
      } catch (error) {
        const e = error as Error;
        console.error(`Failed to read schema file: ${e.message}`);
        throw new Error(
          "Schema must be provided via ZERO_SCHEMA_JSON env var or zero-schema.json file",
        );
      }
    };

    const schemaJson = loadSchemaJson();

    // S3 Bucket
    const replicationBucket = new sst.aws.Bucket(`replication-bucket`, {
      public: false,
    });

    // VPC Configuration
    const vpc = new sst.aws.Vpc(`vpc`, {
      az: 2,
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
      ZERO_SCHEMA_JSON: schemaJson,
      ZERO_LOG_FORMAT: "json",
      ZERO_REPLICA_FILE: "sync-replica.db",
      ZERO_LITESTREAM_BACKUP_URL: $interpolate`s3://${replicationBucket.name}/backup`,
      ZERO_IMAGE_URL:
        process.env.ZERO_IMAGE_URL ||
        `${process.env.AWS_ACCOUNT_ID}.dkr.ecr.${process.env.AWS_REGION}.amazonaws.com/${process.env.ECR_IMAGE_ZERO_CACHE}:latest`,
    };

    if (!commonEnv.ZERO_IMAGE_URL) {
      throw new Error(
        "ZERO_IMAGE_URL is required. Either provide it directly or ensure AWS_ACCOUNT_ID, AWS_REGION, and ECR_IMAGE_ZERO_CACHE are set.",
      );
    }

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
    cluster.addService(`view-syncer`, {
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
    });
  },
});
