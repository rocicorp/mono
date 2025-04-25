/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />
// Load .env file

/**
 * Represents the configuration for an ECS container
 */
interface ContainerDefinition {
  /** The name of the container */
  name: string;
  /** The Docker image to use */
  image?: string;
  /** The amount of CPU to allocate */
  cpu?: string;
  /** The amount of memory to allocate */
  memory?: string;
  /** Container health check configuration */
  health?: {
    command: string[];
    interval: string;
    retries: number;
    startPeriod: string;
  };
  /** Environment variables for the container */
  environment?: Record<string, string| any> 
  /** Logging configuration */
  logging?: {
    retention: string;
  };
  /** Load balancer configuration */
  loadBalancer?: {
    public: boolean;
    ports?: Array<{
      listen: string;
      forward: string;
    }>;
  };
}

/**
 * Returns an array of ECS container definitions:
 *  [ your primary “app” container, plus the OTEL side-car ]
 *
 * Only when you call this will the OTEL IAM Role & Policy be created.
 */
export function withOtelContainers(
  base: ContainerDefinition,
  apiKey: string,
): any[] {
  // 1) Create the OTEL Task Role (only when invoked)
  const otelTaskRole = new aws.iam.Role(`${base.name}-otel-task-role`, {
    assumeRolePolicy: JSON.stringify({
      Version: '2012-10-17',
      Statement: [
        {
          Effect: 'Allow',
          Principal: {Service: 'ecs-tasks.amazonaws.com'},
          Action: 'sts:AssumeRole',
        },
      ],
    }),
  });

  // 2) Attach CloudWatch Logs, X-Ray & SSM permissions
  new aws.iam.RolePolicy(`${base.name}-otel-policy`, {
    role: otelTaskRole.id,
    policy: JSON.stringify({
      Version: '2012-10-17',
      Statement: [
        {
          Effect: 'Allow',
          Action: [
            'logs:CreateLogGroup',
            'logs:CreateLogStream',
            'logs:PutLogEvents',
            'xray:PutTraceSegments',
            'xray:PutTelemetryRecords',
            'ssm:GetParameters',
          ],
          Resource: '*',
        },
      ],
    }),
  });

  // 3) Build your primary application container (just echo back `base`)
  const appContainer = {
    ...base,
    environment: {
      ...base.environment,
      ZERO_LOG_TRACE_COLLECTOR: 'http://localhost:4318/v1/traces',
    },
  };

  // 4) Build the OTEL side-car container
  const otelContainer = {
    name: 'otel',
    image: 'public.ecr.aws/aws-observability/aws-otel-collector:latest',
    cpu: '0.25 vCPU',
    memory: '0.5 GB',
    essential: false,
    taskRole: otelTaskRole.arn,
    environment: {
      OTEL_CONFIG: `
      extensions:
        health_check:
      receivers:
        otlp:
          protocols:
            http:
              endpoint: 0.0.0.0:4318
            grpc:
              endpoint: 0.0.0.0:4317
      
      processors:
        batch/traces:
          timeout: 1s
          send_batch_size: 5
        batch/metrics:
          timeout: 60s
        batch/logs:
          timeout: 60s
        batch/datadog:
          # Datadog APM Intake limit is 3.2MB.    
          send_batch_max_size: 1000
          send_batch_size: 5
          timeout: 10s
        memory_limiter:
          check_interval: 1s
          limit_mib: 1000
      
      exporters:
        debug:
          verbosity: detailed
        awsxray:
        awsemf:
          namespace: ECS/AWSOTel
          log_group_name: '/aws/ecs/otel/zero/metrics'
        datadog/api:
          hostname: zero-sandbox
          api:
            key: ${apiKey}
            site: datadoghq.com
      service:
        pipelines:
          traces:
            receivers: [otlp]
            processors: [batch/datadog]
            exporters: [datadog/api, awsxray]
          metrics:
            receivers: [otlp]
            processors: [batch/metrics]
            exporters: [datadog/api]
          logs:
            receivers: [otlp]
            processors: [batch/datadog]
            exporters: [datadog/api]
        extensions: [health_check]
                `,
    },

    command: [
      '--config=env:OTEL_CONFIG',
      '--feature-gates=-exporter.datadogexporter.DisableAPMStats',
    ],
  };

  // 5) Return both definitions so SST will include them in your Task Definition
  return [appContainer, otelContainer];
}
