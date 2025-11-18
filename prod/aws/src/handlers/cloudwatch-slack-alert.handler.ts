import {gunzipSync} from 'node:zlib';
import {
  CloudWatchLogsClient,
  FilterLogEventsCommand,
} from '@aws-sdk/client-cloudwatch-logs';
import {consoleLogSink, LogContext} from '@rocicorp/logger';

// Configure log group mappings as fallback
const LOG_GROUP_MAPPINGS = {
  alarmPatterns: {
    'Zero Production Cluster Event - Stop':
      '/aws/events/zero-production-cluster-statechange',
    'High ZBugs Error Rate':
      '/sst/cluster/zero-production-clusterCluster/view-syncer/view-syncer',
  } as Record<string, string>,
  namespacePatterns: {
    'cluster/zero-production-clusterCluster':
      '/sst/cluster/zero-production-clusterCluster/view-syncer/view-syncer',
    'ecs/zbugs-production-viewsyncer':
      '/sst/cluster/zero-production-clusterCluster/view-syncer/view-syncer',
  } as Record<string, string>,
};

interface CloudWatchLogsPayload {
  logGroup: string;
  logStream: string;
  logEvents: Array<{
    id: string;
    timestamp: number;
    message: string;
  }>;
}

interface CloudWatchLogsEvent {
  awslogs: {
    data: string;
  };
}

interface AlarmState {
  value: 'ALARM' | 'OK' | 'INSUFFICIENT_DATA';
  reason: string;
  timestamp?: string;
}

interface MetricInfo {
  namespace: string;
  name: string;
  dimensions: Record<string, string>;
}

interface AlarmConfiguration {
  metrics?: Array<{
    id?: string;
    metricStat?: {
      metric: {
        namespace?: string;
        name?: string;
        dimensions?: Record<string, string>;
      };
      period?: number;
      stat?: string;
    };
    expression?: string;
    returnData?: boolean;
  }>;
}

interface AlarmDetail {
  alarmName: string;
  state: AlarmState;
  previousState?: {
    value: string;
  };
  configuration?: AlarmConfiguration;
}

interface AlarmEvent {
  detail?: AlarmDetail;
  alarmData?: AlarmDetail;
  awslogs?: {
    data: string;
  };
}

interface LambdaContext {
  getRemainingTimeInMillis(): number;
}

function parseAwsLogsEvent(event: CloudWatchLogsEvent): CloudWatchLogsPayload {
  const compressed = Buffer.from(event.awslogs.data, 'base64');
  const uncompressed = gunzipSync(compressed);
  return JSON.parse(uncompressed.toString('utf-8'));
}

function getLogGroupFromMapping(
  alarmName: string,
  namespace: string | undefined,
  lc: LogContext,
): string | undefined {
  // Try alarm name pattern matching
  for (const [pattern, logGroup] of Object.entries(
    LOG_GROUP_MAPPINGS.alarmPatterns,
  )) {
    if (alarmName.toLowerCase().includes(pattern.toLowerCase())) {
      lc.info?.(`Matched alarm pattern '${pattern}' -> ${logGroup}`);
      return logGroup;
    }
  }

  // Try namespace pattern matching
  if (namespace) {
    for (const [pattern, logGroup] of Object.entries(
      LOG_GROUP_MAPPINGS.namespacePatterns,
    )) {
      if (namespace.includes(pattern)) {
        lc.info?.(`Matched namespace pattern '${pattern}' -> ${logGroup}`);
        return logGroup;
      }
    }
  }

  return undefined;
}

function extractMetricsFromAlarm(detail: AlarmDetail): MetricInfo[] {
  const metricsInfo: MetricInfo[] = [];

  if (!detail.configuration) {
    return metricsInfo;
  }

  const metrics = detail.configuration.metrics ?? [];

  for (const metric of metrics) {
    // Skip metric math expressions, look for actual metrics
    if (metric.metricStat) {
      const metricStat = metric.metricStat;
      const metricInfo = metricStat.metric;

      const namespace = metricInfo.namespace;
      const name = metricInfo.name;
      const dimensions = metricInfo.dimensions ?? {};

      if (namespace && name) {
        metricsInfo.push({
          namespace,
          name,
          dimensions,
        });
      }
    }
  }

  return metricsInfo;
}

async function fetchErrorLogs(
  logGroup: string,
  startTime: Date,
  endTime: Date,
  lc: LogContext,
  logStream?: string,
  maxLogs = 5,
  timeoutSeconds = 2,
): Promise<string[] | undefined> {
  const logsClient = new CloudWatchLogsClient({
    requestHandler: {
      requestTimeout: timeoutSeconds * 1000,
      httpsAgent: {
        timeout: timeoutSeconds * 1000,
      },
    },
  });

  try {
    const startMs = Math.floor(startTime.getTime());
    const endMs = Math.floor(endTime.getTime());

    const params: {
      logGroupName: string;
      startTime: number;
      endTime: number;
      filterPattern: string;
      limit: number;
      logStreamNames?: string[];
    } = {
      logGroupName: logGroup,
      startTime: startMs,
      endTime: endMs,
      filterPattern: 'ERROR', // Simple text search - much faster
      limit: maxLogs,
    };

    if (logStream) {
      params.logStreamNames = [logStream];
    }

    lc.info?.(`Fetching logs (timeout=${timeoutSeconds}s)`);
    const response = await logsClient.send(new FilterLogEventsCommand(params));

    const errorLogs: string[] = [];
    for (const event of response.events ?? []) {
      const timestamp = new Date(event.timestamp!)
        .toISOString()
        .slice(0, 19)
        .replace('T', ' ');
      let message = event.message?.trim() ?? '';
      if (message.length > 300) {
        message = message.slice(0, 300) + '...';
      }
      errorLogs.push(`[${timestamp}] ${message}`);
    }

    lc.info?.(`Found ${errorLogs.length} error logs`);
    return errorLogs;
  } catch (error) {
    lc.warn?.('Could not fetch logs (may have timed out)', {error});
    return undefined;
  }
}

export async function handle(
  event: AlarmEvent,
  context?: LambdaContext,
): Promise<{statusCode: number; body: string}> {
  const lc = new LogContext('info', {}, consoleLogSink);

  try {
    const alarmName =
      event.detail?.alarmName || event.alarmData?.alarmName || 'unknown';
    lc.info?.(`Received event for alarm: ${alarmName}`);

    const webhookUrl = process.env.SLACK_WEBHOOK;
    if (!webhookUrl) {
      throw new Error('SLACK_WEBHOOK environment variable not set');
    }

    const awsRegion = process.env.AWS_REGION || 'us-east-1';

    let logGroup: string | undefined;
    let logStream: string | undefined;

    // 1) Check if this is a direct CloudWatch Logs event
    if (event.awslogs) {
      const logsPayload = parseAwsLogsEvent(event as CloudWatchLogsEvent);
      logGroup = logsPayload.logGroup;
      if (logsPayload.logEvents && logsPayload.logEvents.length > 0) {
        logStream = logsPayload.logStream;
      }
    }

    // 2) Handle both 'detail' (EventBridge) and 'alarmData' (SNS/direct) formats
    const detail = event.detail || event.alarmData;
    if (!detail) {
      throw new Error('No alarm detail found in event');
    }

    const state = detail.state;
    const newValue = state.value;
    const reason = state.reason;
    const prev = detail.previousState?.value;

    // 3) Extract all metrics from the alarm (handles metric math)
    const metricsInfo = extractMetricsFromAlarm(detail);
    lc.info?.(`Extracted ${metricsInfo.length} metrics from alarm`);

    // 4) Try to find log group from event dimensions first
    for (const metricInfo of metricsInfo) {
      const dimensions = metricInfo.dimensions;
      if (dimensions && 'LogGroupName' in dimensions) {
        logGroup = dimensions['LogGroupName'];
        lc.info?.(`Found log group in dimensions: ${logGroup}`);
        break;
      }
    }

    // 5) Try configured mappings using the first metric's namespace
    if (!logGroup && metricsInfo.length > 0) {
      const firstNamespace = metricsInfo[0].namespace;
      logGroup = getLogGroupFromMapping(detail.alarmName, firstNamespace, lc);
    }

    // 6) Figure out time window - use SHORTER window (5 min before/after)
    let ts: Date;
    const stateChangeTime = state.timestamp;
    if (stateChangeTime) {
      try {
        ts = new Date(stateChangeTime);
      } catch (error) {
        lc.warn?.('Could not parse timestamp', {error});
        ts = new Date();
      }
    } else {
      ts = new Date();
    }

    // Use shorter 5-minute window for faster queries
    const windowMs = 5 * 60 * 1000; // 5 minutes
    const startTime = new Date(ts.getTime() - windowMs);
    const endTime = new Date(ts.getTime() + windowMs);

    // 7) Fetch actual error logs if we found a log group
    let errorLogs: string[] | undefined;
    if (logGroup) {
      lc.info?.(`Fetching logs from: ${logGroup}`);
      // Calculate remaining time (leave 2 seconds for Slack posting)
      const remainingMs = context?.getRemainingTimeInMillis() ?? 5000;
      const timeout = Math.max(1, Math.floor((remainingMs - 2000) / 1000));

      errorLogs = await fetchErrorLogs(
        logGroup,
        startTime,
        endTime,
        lc,
        logStream,
        5,
        timeout,
      );
    } else {
      lc.warn?.(
        `Could not determine log group for alarm '${detail.alarmName}'`,
      );
    }

    // 8) Build console link
    let logLink: string | undefined;
    if (logGroup) {
      const encGroup = encodeURIComponent(logGroup);
      const encFilter = encodeURIComponent('ERROR');
      const startIso = startTime.toISOString();
      const endIso = endTime.toISOString();

      logLink =
        `https://${awsRegion}.console.aws.amazon.com/cloudwatch/home` +
        `?region=${awsRegion}` +
        `#logsV2:log-groups/log-group/${encGroup}/log-events` +
        `?start=${startIso}` +
        `&end=${endIso}` +
        `&filterPattern=${encFilter}`;
    }

    // 9) Skip INSUFFICIENT_DATA and OK->ALARM transitions that we want to ignore
    if (prev === 'INSUFFICIENT_DATA' && newValue !== 'ALARM') {
      lc.info?.('Skipping INSUFFICIENT_DATA -> OK transition');
      return {statusCode: 200, body: 'Skipping INSUFFICIENT_DATA'};
    }

    // 10) Build Slack message
    let text = `*Alarm:* \`${detail.alarmName}\` → *${newValue}*\n${reason}\n`;

    // Add log information if available
    if (errorLogs === undefined && logGroup) {
      text += '\n⚠️ _Could not fetch logs (query timed out or failed)_\n';
    } else if (errorLogs && errorLogs.length > 0) {
      const logsPreview = errorLogs.slice(0, 3).join('\n'); // Show only 3 to keep message short
      text += `\n*Recent Error Logs:*\n\`\`\`\n${logsPreview}\n\`\`\`\n`;
      if (errorLogs.length > 3) {
        text += `\n_(${errorLogs.length - 3} more errors)_\n`;
      }
    } else if (errorLogs !== undefined && errorLogs.length === 0 && logGroup) {
      text += '\n_No ERROR logs in 10-minute window_\n';
    } else if (!logGroup) {
      text += '\nℹ️ _No log group configured_\n';
    }

    if (logLink) {
      text += `\n<${logLink}|View logs in CloudWatch>`;
    }

    // 11) Post to Slack
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({text}),
    });

    lc.info?.(`Posted to Slack: ${response.status}`);

    const body = await response.text();
    return {
      statusCode: response.status,
      body,
    };
  } catch (error) {
    lc.error?.('Error in lambda_handler', {error});
    throw error;
  }
}
