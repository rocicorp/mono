/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />


// Load .env file


interface ContainerDefinition {
  name: string;
  image?: string;
  cpu?: string;
  memory?: string;
  health?: {
    command: string[];
    interval: string;
    retries: number;
    startPeriod: string;
  };
  environment?: Record<string, string| any> 
  logging?: {
    retention: string;
  };
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
  config: {
    apiKey: string,
    appName: string,
    appVersion: string,
  },
): any[] {
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

  const appContainer = {
    ...base,
    environment: {
      ...base.environment,
      ZERO_LOG_TRACE_COLLECTOR: 'http://localhost:4318/v1/traces',
    },
  };

  const otelContainer = {
    name: 'otel',
    image: 'otel/opentelemetry-collector-contrib:0.123.0-amd64',
    cpu: '0.25 vCPU',
    memory: '0.5 GB',
    essential: false,
    taskRole: otelTaskRole.arn,
    environment: {
      OTEL_RESOURCE_ATTRIBUTES: `service.name=${config.appName},service.version=${config.appVersion}`,
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
          send_batch_size: 100
          timeout: 10s
        memory_limiter:
          check_interval: 1s
          limit_mib: 1000
        resourcedetection/env:
          detectors: [env]
          timeout: 2s
          override: false
      connectors:
        datadog/connector:
      exporters:
        debug:
          verbosity: detailed
        awsemf:
          namespace: ECS/AWSOTel
          log_group_name: '/aws/ecs/otel/zero/metrics'
        datadog/api:
          hostname: zero-sandbox
          api:
            key: ${config.apiKey}
            site: datadoghq.com
      service:
        pipelines:
          traces:
            receivers: [otlp]
            processors: [resourcedetection/env, batch/traces]
            exporters: [datadog/connector, datadog/api]
          metrics:
            receivers: [datadog/connector, otlp]
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
    ],
  };

  return [appContainer, otelContainer];
}


export function addServiceWithOtel(
  cluster: sst.aws.Cluster,
  name: string,
  serviceProps: Omit<any, "containers"> & { containers: any[] },
  config: { apiKey: string; appName: string; appVersion: string }
) {
  const { containers, ...restProps } = serviceProps;

  if (!containers || containers.length === 0) {
    throw new Error(
      "addServiceWithOtel requires at least one container definition in 'containers'"
    );
  }

  const [baseContainer, ...extraContainers] = containers;

  // Generate the OTEL-enhanced container definitions
  const otelContainers = withOtelContainers(baseContainer, config);

  return cluster.addService(name, {
    ...restProps,
    containers: [...otelContainers, ...extraContainers],
  });
}
