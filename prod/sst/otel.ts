/* eslint-disable */
/// <reference path="./.sst/platform/config.d.ts" />

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
  environment?: Record<string, string | any>;
  logging?: {
    retention: string;
  };
  loadBalancer?: {
    public: boolean;
    domain?: {
      name: string;
      dns: boolean;
      cert: string;
    };
    ports?: Array<{
      listen: string;
      forward: string;
      container?: string;
    }>;
  };
  // any other fields you might put on your base container
  [key: string]: any;
}
type ServiceProps = {
  cluster: sst.aws.Cluster;
  containers?: ContainerDefinition[];
  image?: string;
  cpu?:
    | '0.25 vCPU'
    | '0.5 vCPU'
    | '1 vCPU'
    | '2 vCPU'
    | '4 vCPU'
    | '8 vCPU'
    | '16 vCPU';
  memory?: `${number} GB`;
  health?: ContainerDefinition['health'];
  environment?: ContainerDefinition['environment'];
  logging?: ContainerDefinition['logging'];
  loadBalancer?: ContainerDefinition['loadBalancer'];
  [key: string]: any;
};
/**
 * Returns an array of ECS container definitions:
 *  [ your primary "app" container, plus the OTEL side-car ]
 *
 * Only when you call this will the OTEL IAM Role & Policy be created.
 */
export function withOtelContainers(
  base: ContainerDefinition,
  config: {
    apiKey: string;
    appName: string;
    appVersion: string;
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
      ZERO_LOG_METRIC_COLLECTOR: 'http://localhost:4318/v1/metrics',
      ZERO_LOG_LOG_COLLECTOR: 'http://localhost:4318/v1/logs',
    },
  };

  const otelContainer = {
    name: 'otel',
    image: 'otel/opentelemetry-collector-contrib:0.123.0-amd64',
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
        transform:
          trace_statements:
            - context: resource
              statements:
                - set(attributes["datadog.host.name"], "${config.appName}")
                - set(attributes["datadog.host.version"], "${config.appVersion}")
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
            processors: [transform, resourcedetection/env, batch/traces]
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

    command: ['--config=env:OTEL_CONFIG'],
  };

  return [appContainer, otelContainer];
}
export function addServiceWithOtel(
  serviceName: string,
  props: ServiceProps,
): sst.aws.Service {
  const {
    cluster,
    containers: propContainers,
    image,
    cpu,
    memory,
    health,
    environment,
    logging,
    loadBalancer,
    ...otherServiceProps
  } = props;

  //Normalize loadbalancer
  const noramlizedLoadbalancer = {
    public: loadBalancer.public,
    domain: loadBalancer.domain,
    ports: loadBalancer.ports.map(port => ({
      ...port,
      container: serviceName,
    })),
  };

  //Normalize into one ContainerDefinition[]
  let containers: ContainerDefinition[];
  if (Array.isArray(propContainers)) {
    if (propContainers.length === 0) {
      throw new Error('`containers` must be non-empty');
    }
    containers = propContainers;
  } else {
    if (!image) {
      throw new Error('Either `containers` or top-level `image` is required');
    }

    containers = [{name: serviceName, image, health, environment, logging}];
  }

  const otelConfig = {
    apiKey: process.env.DATADOG_API_KEY!,
    appName: `${$app.name}-${$app.stage}`,
    appVersion: process.env.ZERO_IMAGE_URL!,
  };

  const [primary, ...extraContainers] = containers;
  const otelSidecars = withOtelContainers(primary, otelConfig);

  return new sst.aws.Service(serviceName, {
    cluster,
    cpu,
    memory,
    loadBalancer: noramlizedLoadbalancer as any,
    ...otherServiceProps,
    containers: [...otelSidecars, ...extraContainers],
  });
}
