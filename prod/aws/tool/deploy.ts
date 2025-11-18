/* eslint-disable @typescript-eslint/naming-convention */
import {
  CreateAliasCommand,
  DeleteFunctionCommand,
  LambdaClient,
  ListAliasesCommand,
  ListVersionsByFunctionCommand,
  UpdateAliasCommand,
  AddPermissionCommand,
  type CreateAliasCommandOutput,
} from '@aws-sdk/client-lambda';
import {
  CloudWatchLogsClient,
  PutMetricFilterCommand,
  DescribeMetricFiltersCommand,
} from '@aws-sdk/client-cloudwatch-logs';
import {
  CloudWatchClient,
  PutMetricAlarmCommand,
  DescribeAlarmsCommand,
} from '@aws-sdk/client-cloudwatch';
import type {StandardUnit} from '@aws-sdk/client-cloudwatch-logs';
import {
  IAMClient,
  CreateRoleCommand,
  GetRoleCommand,
  PutRolePolicyCommand,
  AttachRolePolicyCommand,
} from '@aws-sdk/client-iam';
import {STSClient, GetCallerIdentityCommand} from '@aws-sdk/client-sts';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {execSync} from 'node:child_process';
import {readdirSync, renameSync} from 'node:fs';
import pkg from '../package.json' with {type: 'json'};

const STABLE_RELEASE = 'stable';

async function getAccountID(): Promise<string> {
  const client = new STSClient();
  const resp = await client.send(new GetCallerIdentityCommand({}));
  if (!resp.Account) {
    throw new Error('Could not determine AWS account ID');
  }
  return resp.Account;
}

const accountID = await getAccountID();

// All lambda roles must allow lambda to assume the role.
const assumeRolePolicyDef = {
  Version: '2012-10-17',
  Statement: [
    {
      Effect: 'Allow',
      Principal: {Service: 'lambda.amazonaws.com'},
      Action: 'sts:AssumeRole',
    },
  ],
} as const;

function roleName(lambdaName: string) {
  return `${lambdaName}-role`;
}

/**
 * Metric filter configuration
 */
interface MetricFilterConfig {
  /** Metric filter name */
  filterName: string;
  /** Log group to monitor */
  logGroupName: string;
  /** Filter pattern to match in logs */
  filterPattern: string;
  /** Metric namespace */
  metricNamespace: string;
  /** Metric name */
  metricName: string;
  /** Metric value (default: "1") */
  metricValue?: string;
  /** Default value when no match (default: 0) */
  defaultValue?: number;
  /** Unit for the metric */
  unit?: StandardUnit | undefined;
  /** Dimensions for the metric */
  dimensions?: Record<string, string>;
}

/**
 * Configuration for a lambda function's IAM role and alarm setup
 */
interface LambdaConfig {
  /** Inline IAM policies to attach to the role */
  inlinePolicies?: Record<string, unknown>;
  /** Managed AWS policy ARNs to attach to the role */
  managedPolicyARNs?: string[];
  /** Metric filters to create (independent of alarms) */
  metricFilters?: MetricFilterConfig[];
  /** CloudWatch alarm configuration (if this lambda should be triggered by alarms) */
  alarmConfig?: {
    /** Log group to monitor */
    logGroupName: string;
    /** Metric filter name */
    metricFilterName: string;
    /** Metric namespace */
    metricNamespace: string;
    /** Metric name */
    metricName: string;
    /** Filter pattern to match in logs */
    filterPattern: string;
    /** Alarm name */
    alarmName: string;
    /** Alarm description */
    alarmDescription: string;
    /** Threshold for triggering alarm */
    threshold?: number;
    /** Evaluation periods */
    evaluationPeriods?: number;
  };
}

/**
 * Define configuration for each lambda
 * Add your lambda configurations here
 */
const lambdaConfigs: Record<string, LambdaConfig> = {
  'log-alert': {
    managedPolicyARNs: [
      'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
    ],
    alarmConfig: {
      logGroupName: '/aws/lambda/example-function', // TODO: Update with actual log group
      metricFilterName: 'error-metric-filter',
      metricNamespace: 'CustomMetrics/Errors',
      metricName: 'ErrorCount',
      filterPattern: '[time, request_id, event_type = "ERROR", ...]',
      alarmName: 'lambda-error-alarm',
      alarmDescription: 'Triggers when errors are detected in logs',
      threshold: 1,
      evaluationPeriods: 1,
    },
  },
  'slack-alert': {
    managedPolicyARNs: [
      'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
    ],
    // Add alarm config here if needed
  },
  'cloudwatch-slack-alert': {
    managedPolicyARNs: [
      'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
    ],
    inlinePolicies: {
      'cloudwatch-logs-and-alarms': {
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'ReadLogsAndFetchEvents',
            Effect: 'Allow',
            Action: [
              'logs:FilterLogEvents',
              'logs:GetLogEvents',
              'logs:DescribeLogStreams',
              'logs:DescribeLogGroups',
              'logs:DescribeMetricFilters',
            ],
            Resource: '*', // TODO: Restrict to specific log groups
          },
          {
            Sid: 'DescribeAlarms',
            Effect: 'Allow',
            Action: ['cloudwatch:DescribeAlarms'],
            Resource: '*',
          },
        ],
      },
    },
    // Metric filters for ZBugs production viewsyncer
    metricFilters: [
      {
        filterName: 'prod_viewsyncer_error_count',
        logGroupName:
          '/sst/cluster/zero-production-clusterCluster/view-syncer/view-syncer',
        filterPattern: '{ $.level = "ERROR" }',
        metricNamespace: 'ecs/zbugs-production-viewsyncer',
        metricName: 'prod_viewsyncer_error_count',
        metricValue: '1',
        defaultValue: 0,
      },
      {
        filterName: 'zbugs_prod_viewsyncer_info_count',
        logGroupName:
          '/sst/cluster/zero-production-clusterCluster/view-syncer/view-syncer',
        filterPattern: '{ $.level = "INFO" }',
        metricNamespace: 'ecs/zbugs-production-viewsyncer',
        metricName: 'zbugs_prod_viewsyncer_info_count',
        metricValue: '1',
        defaultValue: 0,
      },
      {
        filterName: 'zbugs_prod_viewsyncer_level_count',
        logGroupName:
          '/sst/cluster/zero-production-clusterCluster/view-syncer/view-syncer',
        filterPattern: '{ $.level = "INFO" || $.level = "ERROR" }',
        metricNamespace: 'ecs/zbugs-production-viewsyncer',
        metricName: 'zbugs_prod_viewsyncer_level_count',
        metricValue: '$.level',
        dimensions: {
          level: '$.level',
        },
      },
    ],
    // This lambda is triggered directly by CloudWatch alarms
    // Set AlarmActions: [lambdaArn] in your existing alarm configuration
  },
};

/**
 * Setup IAM role for a lambda function
 */
async function setupRole(
  lc: LogContext,
  lambdaName: string,
  config: LambdaConfig,
): Promise<void> {
  const client = new IAMClient();
  const role = roleName(lambdaName);

  let roleExists = false;
  try {
    await client.send(new GetRoleCommand({RoleName: role}));
    roleExists = true;
    lc.info?.(`Role ${role} already exists`);
  } catch (e) {
    lc.info?.(`Creating role ${role}`);
  }

  if (!roleExists) {
    await client.send(
      new CreateRoleCommand({
        RoleName: role,
        AssumeRolePolicyDocument: JSON.stringify(assumeRolePolicyDef),
        Description: `Role for ${lambdaName} lambda function`,
      }),
    );
  }

  // Attach managed policies
  if (config.managedPolicyARNs) {
    for (const policyArn of config.managedPolicyARNs) {
      await client.send(
        new AttachRolePolicyCommand({
          RoleName: role,
          PolicyArn: policyArn,
        }),
      );
      lc.debug?.(`Attached managed policy ${policyArn} to ${role}`);
    }
  }

  // Add inline policies
  if (config.inlinePolicies) {
    for (const [policyName, policyDoc] of Object.entries(
      config.inlinePolicies,
    )) {
      await client.send(
        new PutRolePolicyCommand({
          RoleName: role,
          PolicyName: policyName,
          PolicyDocument: JSON.stringify(policyDoc),
        }),
      );
      lc.debug?.(`Added inline policy ${policyName} to ${role}`);
    }
  }
}

/**
 * Setup CloudWatch metric filter for a log group
 */
async function setupMetricFilter(
  lc: LogContext,
  config: MetricFilterConfig,
): Promise<void> {
  const client = new CloudWatchLogsClient();

  // Check if metric filter already exists
  const existing = await client.send(
    new DescribeMetricFiltersCommand({
      logGroupName: config.logGroupName,
      filterNamePrefix: config.filterName,
    }),
  );

  if (existing.metricFilters?.length) {
    lc.info?.(`Metric filter ${config.filterName} already exists, updating...`);
  } else {
    lc.info?.(`Creating metric filter ${config.filterName}`);
  }

  const transformation: {
    metricName: string;
    metricNamespace: string;
    metricValue: string;
    defaultValue?: number | undefined;
    unit?: StandardUnit | undefined;
    dimensions?: Record<string, string> | undefined;
  } = {
    metricName: config.metricName,
    metricNamespace: config.metricNamespace,
    metricValue: config.metricValue ?? '1',
  };

  if (config.defaultValue !== undefined) {
    transformation.defaultValue = config.defaultValue;
  }

  if (config.unit) {
    transformation.unit = config.unit;
  }

  if (config.dimensions) {
    transformation.dimensions = config.dimensions;
  }

  await client.send(
    new PutMetricFilterCommand({
      logGroupName: config.logGroupName,
      filterName: config.filterName,
      filterPattern: config.filterPattern,
      metricTransformations: [transformation],
    }),
  );

  lc.info?.(`Metric filter ${config.filterName} configured`);
}

/**
 * Setup CloudWatch alarm and wire it to invoke the lambda directly
 */
async function setupAlarm(
  lc: LogContext,
  lambdaName: string,
  lambdaArn: string,
  config: NonNullable<LambdaConfig['alarmConfig']>,
): Promise<void> {
  const cwClient = new CloudWatchClient();
  const lambdaClient = new LambdaClient();

  // 1. Grant CloudWatch Alarms permission to invoke the Lambda
  // This must be done BEFORE creating the alarm
  try {
    await lambdaClient.send(
      new AddPermissionCommand({
        FunctionName: lambdaName,
        StatementId: `AllowCloudWatchAlarm-${config.alarmName}`,
        Action: 'lambda:InvokeFunction',
        Principal: 'lambda.alarms.cloudwatch.amazonaws.com',
        SourceAccount: accountID,
      }),
    );
    lc.info?.(`Granted CloudWatch Alarms permission to invoke ${lambdaName}`);
  } catch (e) {
    // Permission might already exist
    if (e instanceof Error && e.message.includes('ResourceConflictException')) {
      lc.debug?.(
        `CloudWatch Alarms permission already exists for ${lambdaName}`,
      );
    } else {
      throw e;
    }
  }

  // 2. Create or update the CloudWatch alarm with Lambda as action
  const existing = await cwClient.send(
    new DescribeAlarmsCommand({
      AlarmNames: [config.alarmName],
    }),
  );

  if (existing.MetricAlarms?.length) {
    lc.info?.(`Alarm ${config.alarmName} already exists, updating...`);
  } else {
    lc.info?.(`Creating alarm ${config.alarmName}`);
  }

  // CloudWatch alarms can directly invoke Lambda functions
  await cwClient.send(
    new PutMetricAlarmCommand({
      AlarmName: config.alarmName,
      AlarmDescription: config.alarmDescription,
      MetricName: config.metricName,
      Namespace: config.metricNamespace,
      Statistic: 'Sum',
      Period: 60, // 1 minute
      EvaluationPeriods: config.evaluationPeriods ?? 1,
      Threshold: config.threshold ?? 1,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
      TreatMissingData: 'notBreaching',
      ActionsEnabled: true,
      AlarmActions: [lambdaArn], // Directly invoke Lambda on ALARM state
      OKActions: [lambdaArn], // Also notify on OK state
    }),
  );

  lc.info?.(
    `✓ Complete: Alarm ${config.alarmName} -> Lambda ${lambdaName} (direct invocation)`,
  );
}

/**
 * Builds, packages, and deploys all lambdas in the src/handlers directory
 */
async function deploy(lc: LogContext, lambdas: string[]) {
  lc.debug?.(`Deploying lambdas`, {lambdas});

  const tarball = `${pkg.name}-${pkg.version}.tgz`;

  // npm pack creates a gzipped bundle containing the necessary contents in
  // the "package" directory
  execSync('npm pack');

  // gunzip it
  execSync('mkdir -p dist');
  execSync('rm -rf dist/*');
  execSync(`mv ${tarball} dist`);
  execSync(`tar -xvf ${tarball}`, {cwd: 'dist'});

  // then create a zip for each lambda with the entry point and node_modules.
  for (const name of lambdas) {
    for (const suffix of ['', '.map']) {
      renameSync(
        `dist/package/out/${name}.handler.mjs${suffix}`,
        `dist/package/out/${name}.mjs${suffix}`,
      );
    }
    const zipfile = `${name}.zip`;
    execSync(
      `zip -r ${zipfile} out/${name}.mjs out/${name}.mjs.map node_modules`,
      {cwd: `dist/package`},
    );
    await setupFunction(lc, name, `dist/package/${zipfile}`);
  }
}

async function getAliasedFunctionVersions(
  lc: LogContext,
  client: LambdaClient,
  name: string,
) {
  const versions = new Set<string>();
  let marker: string | undefined;
  for (let first = true; first || marker !== undefined; first = false) {
    const resp = await client.send(
      new ListAliasesCommand({
        FunctionName: name,
        Marker: marker,
        MaxItems: 50,
      }),
    );
    marker = resp.NextMarker;
    (resp.Aliases ?? [])
      .map(
        (alias: {FunctionVersion?: string | undefined}) =>
          alias.FunctionVersion,
      )
      .filter((v: string | undefined): v is string => v !== undefined)
      .forEach((v: string) => versions.add(v));
  }
  lc.debug?.(`Aliased versions of ${name}`, [...versions]);
  return versions;
}

async function deleteInactiveUnaliasedFunctionVersions(
  lc: LogContext,
  client: LambdaClient,
  name: string,
) {
  const aliased = await getAliasedFunctionVersions(lc, client, name);
  let marker: string | undefined;
  for (let first = true; first || marker !== undefined; first = false) {
    const resp = await client.send(
      new ListVersionsByFunctionCommand({
        FunctionName: name,
        Marker: marker,
        MaxItems: 50,
      }),
    );
    for (const fn of resp.Versions ?? []) {
      const version = fn.Version;
      if (!version) continue;

      if (version !== '$LATEST' && !aliased.has(version)) {
        if (fn.State === 'Active' || fn.State === 'Pending') {
          lc.warn?.(
            `Unaliased function ${name}@${version} is ${fn.State}. Not deleting.`,
          );
        } else {
          await client.send(
            new DeleteFunctionCommand({
              FunctionName: name,
              Qualifier: version,
            }),
          );
          lc.info?.(`Deleted obsolete function version ${name}@${version}`);
        }
      }
    }
  }
}

async function setupFunction(lc: LogContext, name: string, zipFile: string) {
  const client = new LambdaClient();
  let exists = false;
  try {
    execSync(`aws lambda get-function --function-name ${name}`, {
      stdio: 'ignore',
    });
    exists = true;
    lc.info?.(`Updating function ${name}`);
  } catch (e) {
    lc.info?.(`Creating function ${name}`);
  }

  const envVars: Record<string, string> = {
    NODE_OPTIONS: '--enable-source-maps',
    LOG_LEVEL: 'info',
    LOG_FORMAT: 'json',
    // Add SLACK_WEBHOOK from environment if available
    ...(process.env.SLACK_WEBHOOK && {
      SLACK_WEBHOOK: process.env.SLACK_WEBHOOK,
    }),
  };

  const envVariables = JSON.stringify({Variables: envVars});

  let released: CreateAliasCommandOutput;

  if (exists) {
    execSync(
      [
        'aws',
        'lambda',
        'update-function-configuration',
        `--function-name=${name}`,
        '--timeout=180',
        '--runtime=nodejs22.x',
        `--handler=./out/${name}.handle`,
        `--role=arn:aws:iam::${accountID}:role/${roleName(name)}`,
        `--environment='${envVariables}'`,
      ].join(' '),
    );
    execSync(
      [
        'aws',
        'lambda',
        'wait',
        'function-updated',
        `--function-name=${name}`,
      ].join(' '),
    );
    const version = execSync(
      [
        'aws',
        'lambda',
        'update-function-code',
        `--function-name=${name}`,
        `--zip-file=fileb://${zipFile}`,
        `--publish`,
        `--output=text`,
        `--query=Version`,
      ].join(' '),
    )
      .toString()
      .trim();

    released = await client.send(
      new UpdateAliasCommand({
        Name: STABLE_RELEASE,
        FunctionName: name,
        FunctionVersion: version,
      }),
    );
  } else {
    const version = execSync(
      [
        'aws',
        'lambda',
        'create-function',
        `--function-name=${name}`,
        '--timeout=180',
        '--runtime=nodejs22.x',
        `--handler=./out/${name}.handle`,
        `--role=arn:aws:iam::${accountID}:role/${roleName(name)}`,
        `--zip-file=fileb://${zipFile}`,
        `--environment='${envVariables}'`,
        `--publish`,
        `--output=text`,
        `--query=Version`,
      ].join(' '),
    )
      .toString()
      .trim();

    execSync(
      [
        'aws',
        'lambda',
        'wait',
        'function-exists',
        `--function-name=${name}`,
      ].join(' '),
    );

    released = await client.send(
      new CreateAliasCommand({
        Name: STABLE_RELEASE,
        FunctionName: name,
        FunctionVersion: version,
      }),
    );
  }
  lc.info?.(
    `Released ${released.AliasArn}@v${released.FunctionVersion}`,
    released,
  );

  await deleteInactiveUnaliasedFunctionVersions(lc, client, name);

  // Setup metric filters if configured
  const config = lambdaConfigs[name];
  if (config?.metricFilters) {
    lc.info?.(
      `Setting up ${config.metricFilters.length} metric filter(s) for ${name}`,
    );
    for (const filterConfig of config.metricFilters) {
      await setupMetricFilter(lc, filterConfig);
    }
  }

  // Setup CloudWatch alarm if configured
  if (config?.alarmConfig) {
    lc.info?.(`Setting up CloudWatch alarm for ${name}`);
    const alarmFilterConfig: MetricFilterConfig = {
      filterName: config.alarmConfig.metricFilterName,
      logGroupName: config.alarmConfig.logGroupName,
      filterPattern: config.alarmConfig.filterPattern,
      metricNamespace: config.alarmConfig.metricNamespace,
      metricName: config.alarmConfig.metricName,
      metricValue: '1',
      defaultValue: 0,
    };
    await setupMetricFilter(lc, alarmFilterConfig);
    await setupAlarm(lc, name, released.AliasArn!, config.alarmConfig);
  }
}

// Main execution
const lc = new LogContext('debug', {}, consoleLogSink);

// Discover all lambda handlers
const lambdas = readdirSync('./src/handlers')
  .filter(file => file.endsWith('.handler.ts'))
  .map(file => file.replaceAll('.handler.ts', ''));

lc.info?.('Deploying lambdas', {lambdas});

// Setup IAM roles for each lambda
for (const lambda of lambdas) {
  const config = lambdaConfigs[lambda];
  if (!config) {
    lc.warn?.(`No configuration found for lambda ${lambda}, using defaults`);
    await setupRole(lc, lambda, {
      managedPolicyARNs: [
        'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
      ],
    });
  } else {
    await setupRole(lc, lambda, config);
  }
}

// Deploy all lambdas
await deploy(lc, lambdas);

lc.info?.('Deployment complete!');
