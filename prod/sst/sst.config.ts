/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />
import { readFileSync } from "fs";

export default $config({
  app(input) {
    return {
      name: "zero",
      removal: input?.stage === "production" ? "retain" : "remove",
      home: "aws",
      region: process.env.AWS_REGION || "us-east-1",
    };
  },
  async run() {
    // Load .env file
    require("dotenv").config();

    const loadSchemaJson = () => {
      if (process.env.ZERO_SCHEMA_JSON) {
        return process.env.ZERO_SCHEMA_JSON;
      }

      try {
        return readFileSync("zero-schema.json", "utf8");
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
    };

    // Replication Manager Service
    const replicationManager = cluster.addService(`replication-manager`, {
      cpu: "2 vCPU",
      memory: "8 GB",
      image: `${process.env.AWS_ACCOUNT_ID}.dkr.ecr.${process.env.AWS_REGION}.amazonaws.com/${process.env.ECR_IMAGE_ZERO_CACHE}:latest`,
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
    });

    // View Syncer Service
    cluster.addService(`view-syncer`, {
      cpu: "2 vCPU",
      memory: "8 GB",
      image: `${process.env.AWS_ACCOUNT_ID}.dkr.ecr.${process.env.AWS_REGION}.amazonaws.com/${process.env.ECR_IMAGE_ZERO_CACHE}:latest`,
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
        ports: [
          {
            listen: "80/http",
            forward: "4848/http",
          },
        ],
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
