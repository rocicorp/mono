import type {LogContext} from '@rocicorp/logger';
import type {PostgresDB} from '../../../types/pg.ts';
import type {ShardConfig} from '../../../types/shards.ts';

export type ProviderKind =
  | 'aws-aurora'
  | 'aws-rds'
  | 'google-cloud-sql'
  | 'azure-flexible-server'
  | 'neon'
  | 'supabase'
  | 'planetscale'
  | 'render'
  | 'crunchy-bridge'
  | 'aiven'
  | 'digitalocean'
  | 'ibm-cloud'
  | 'unknown';

export type ProviderInfo = {
  kind: ProviderKind;
  name: string;
};

export type EndpointType = 'likely-direct' | 'likely-pooled' | 'unknown';

export type EndpointInfo = {
  host?: string | undefined;
  port?: string | undefined;
  database?: string | undefined;
  endpointType: EndpointType;
  reasons: string[];
};

export type ServerSettings = {
  serverVersion?: string | undefined;
  serverVersionNum?: number | undefined;
  walLevel?: string | null | undefined;
  maxReplicationSlots?: number | undefined;
  maxWalSenders?: number | undefined;
  maxSlotWalKeepSize?: string | null | undefined;
  rdsLogicalReplication?: string | null | undefined;
  cloudsqlLogicalDecoding?: string | null | undefined;
  cloudsqlEnablePglogical?: string | null | undefined;
  syncReplicationSlots?: string | null | undefined;
  hotStandbyFeedback?: string | null | undefined;
  inRecovery?: boolean | undefined;
};

export type RoleInspection = {
  currentUser: string;
  sessionUser: string;
  isSuperuser: boolean;
  hasReplication: boolean;
  bypassRLS: boolean;
  canCreateInDatabase: boolean;
  canConnectToDatabase: boolean;
  memberOfCloudSQLSuperuser: boolean;
  memberOfNeonSuperuser: boolean;
  memberOfRdsReplication: boolean;
  memberOfRdsSuperuser: boolean;
};

export type SlotInspection = {
  totalSlots: number;
  activeSlots: number;
  logicalSlots: number;
};

export type WalSenderInspection = {
  activeSenders: number;
};

export type PublicationInspection = {
  requested: readonly string[];
  existing: PublicationRow[];
  tablePrivileges: PublicationTablePrivilege[];
};

type PublicationRow = {
  name: string;
  publishesInsert: boolean;
  publishesUpdate: boolean;
  publishesDelete: boolean;
  publishesTruncate: boolean;
};

type PublicationTablePrivilege = {
  schema: string;
  table: string;
  hasSchemaUsage: boolean;
  hasSelect: boolean;
  rlsEnabled: boolean;
  rlsForced: boolean;
  owner: string;
  isOwnerOrMember: boolean;
};

export type PreflightInput = {
  provider: ProviderInfo;
  endpoint: EndpointInfo;
  settings?: ServerSettings | undefined;
  role?: RoleInspection | undefined;
  slots?: SlotInspection | undefined;
  walSenders?: WalSenderInspection | undefined;
  publications?: PublicationInspection | undefined;
  requestedPublications: readonly string[];
  replicationSlotFailover?: boolean | undefined;
};

export type PreflightFinding = {
  level: 'info' | 'warning';
  code: string;
  message: string;
  action?: string | undefined;
  details?: Record<string, unknown> | undefined;
};

export type PassiveReplicationPreflightOptions = {
  replicationSlotFailover?: boolean | undefined;
  force?: boolean | undefined;
};

const completedPreflightKeys = new Set<string>();

export async function runPassiveReplicationPreflight(
  lc: LogContext,
  sql: PostgresDB,
  upstreamURI: string,
  shard: ShardConfig,
  options: PassiveReplicationPreflightOptions = {},
): Promise<void> {
  try {
    const endpoint = classifyEndpoint(upstreamURI);
    const key = preflightKey(endpoint, shard);
    if (!options.force && completedPreflightKeys.has(key)) {
      return;
    }
    completedPreflightKeys.add(key);

    const settings = await inspect(lc, 'server replication settings', () =>
      inspectServerSettings(sql),
    );
    const provider = detectProvider(endpoint.host, settings);
    const [role, slots, walSenders, publications] = await Promise.all([
      inspect(lc, 'current role privileges', () => inspectRole(sql)),
      inspect(lc, 'replication slot usage', () => inspectSlots(sql)),
      inspect(lc, 'WAL sender usage', () => inspectWalSenders(sql)),
      inspect(lc, 'configured publications', () =>
        inspectPublications(sql, shard.publications),
      ),
    ]);

    const findings = buildReplicationPreflightFindings({
      provider,
      endpoint,
      settings,
      role,
      slots,
      walSenders,
      publications,
      requestedPublications: shard.publications,
      replicationSlotFailover: options.replicationSlotFailover,
    });
    logFindings(lc, provider, endpoint, findings);
  } catch (e) {
    lc.warn?.(
      `Postgres replication preflight failed unexpectedly; continuing startup`,
      e,
    );
  }
}

export function classifyEndpoint(upstreamURI: string): EndpointInfo {
  let url: URL;
  try {
    url = new URL(upstreamURI);
  } catch {
    return {endpointType: 'unknown', reasons: ['unable to parse upstream URI']};
  }

  const host = url.hostname.toLowerCase();
  const port = url.port || undefined;
  const reasons: string[] = [];
  if (
    host.includes('pooler') ||
    host.includes('pgbouncer') ||
    host.includes('supavisor')
  ) {
    reasons.push('hostname looks like a pooler');
  }
  if (host.includes('-pooler.')) {
    reasons.push('hostname uses provider pooler naming');
  }
  if (port === '6432' || port === '6543') {
    reasons.push(`port ${port} is commonly used by Postgres poolers`);
  }

  return {
    host,
    port,
    database: url.pathname.length > 1 ? url.pathname.slice(1) : undefined,
    endpointType: reasons.length ? 'likely-pooled' : 'likely-direct',
    reasons,
  };
}

export function detectProvider(
  host: string | undefined,
  settings?: ServerSettings | undefined,
): ProviderInfo {
  const normalized = host?.toLowerCase() ?? '';
  if (hasSetting(settings?.cloudsqlLogicalDecoding)) {
    return {kind: 'google-cloud-sql', name: 'Google Cloud SQL'};
  }
  if (hasSetting(settings?.rdsLogicalReplication)) {
    return normalized.includes('cluster-')
      ? {kind: 'aws-aurora', name: 'Amazon Aurora PostgreSQL'}
      : {kind: 'aws-rds', name: 'Amazon RDS for PostgreSQL'};
  }
  if (normalized.endsWith('.neon.tech')) {
    return {kind: 'neon', name: 'Neon'};
  }
  if (
    normalized.endsWith('.supabase.co') ||
    normalized.endsWith('.supabase.com') ||
    normalized.includes('.pooler.supabase.')
  ) {
    return {kind: 'supabase', name: 'Supabase'};
  }
  if (
    normalized.endsWith('.psdb.cloud') ||
    normalized.includes('planetscale')
  ) {
    return {kind: 'planetscale', name: 'PlanetScale Postgres'};
  }
  if (normalized.endsWith('.postgres.database.azure.com')) {
    return {
      kind: 'azure-flexible-server',
      name: 'Azure Database for PostgreSQL Flexible Server',
    };
  }
  if (normalized.endsWith('.rds.amazonaws.com')) {
    return normalized.includes('cluster-')
      ? {kind: 'aws-aurora', name: 'Amazon Aurora PostgreSQL'}
      : {kind: 'aws-rds', name: 'Amazon RDS for PostgreSQL'};
  }
  if (
    normalized.includes('cloudsql') ||
    normalized.endsWith('.cloudsql.googleusercontent.com')
  ) {
    return {kind: 'google-cloud-sql', name: 'Google Cloud SQL'};
  }
  if (normalized.includes('postgres.render.com')) {
    return {kind: 'render', name: 'Render Postgres'};
  }
  if (
    normalized.includes('crunchybridge') ||
    normalized.includes('crunchydata')
  ) {
    return {kind: 'crunchy-bridge', name: 'Crunchy Bridge'};
  }
  if (normalized.endsWith('.aivencloud.com')) {
    return {kind: 'aiven', name: 'Aiven for PostgreSQL'};
  }
  if (normalized.endsWith('.ondigitalocean.com')) {
    return {kind: 'digitalocean', name: 'DigitalOcean Managed PostgreSQL'};
  }
  if (normalized.endsWith('.databases.appdomain.cloud')) {
    return {kind: 'ibm-cloud', name: 'IBM Cloud Databases for PostgreSQL'};
  }
  return {kind: 'unknown', name: 'Unknown PostgreSQL provider'};
}

export function buildReplicationPreflightFindings({
  provider,
  endpoint,
  settings,
  role,
  slots,
  walSenders,
  publications,
  requestedPublications,
  replicationSlotFailover,
}: PreflightInput): PreflightFinding[] {
  const findings: PreflightFinding[] = [
    providerNote(provider, endpoint),
  ].filter(finding => finding !== undefined);

  if (endpoint.endpointType === 'likely-pooled') {
    findings.push({
      level: 'warning',
      code: 'pooled_endpoint',
      message:
        `The upstream Postgres endpoint looks like a pooler, which is unsafe ` +
        `for logical replication.`,
      action: `Use the provider's direct PostgreSQL endpoint for zero-cache logical replication.`,
      details: {reasons: endpoint.reasons},
    });
  }

  if (settings) {
    if (settings.walLevel !== 'logical') {
      findings.push({
        level: 'warning',
        code: 'wal_level_not_logical',
        message: `Postgres wal_level is ${settings.walLevel ?? 'unknown'}, not logical.`,
        action: walLevelAction(provider),
      });
    }
    if (settings.inRecovery) {
      findings.push({
        level: 'warning',
        code: 'source_is_replica',
        message:
          `The upstream connection is to a read replica. Logical replication ` +
          `from replicas is provider and PostgreSQL-version specific.`,
        action: `Prefer the primary writer endpoint unless the provider explicitly supports logical publishing from this replica.`,
      });
    }
    if (
      replicationSlotFailover &&
      (settings.serverVersionNum === undefined ||
        settings.serverVersionNum < 170000)
    ) {
      findings.push({
        level: 'warning',
        code: 'failover_slots_require_pg17',
        message: `Replication slot failover was requested, but PostgreSQL 17+ was not detected.`,
        action: `Upgrade Postgres or disable ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER until the provider supports failover slots.`,
        details: {serverVersion: settings.serverVersion},
      });
    }
    if (
      replicationSlotFailover &&
      settings.serverVersionNum !== undefined &&
      settings.serverVersionNum >= 170000 &&
      settings.syncReplicationSlots !== undefined &&
      settings.syncReplicationSlots !== null &&
      settings.syncReplicationSlots !== 'on'
    ) {
      findings.push({
        level: 'warning',
        code: 'sync_replication_slots_off',
        message: `Replication slot failover was requested, but sync_replication_slots is not on.`,
        action: `Enable the provider's PostgreSQL 17 logical slot synchronization settings before relying on failover slots.`,
        details: {syncReplicationSlots: settings.syncReplicationSlots},
      });
    }
  } else {
    findings.push({
      level: 'warning',
      code: 'settings_unavailable',
      message: `Could not inspect Postgres replication settings.`,
      action: `Verify wal_level, max_replication_slots, and max_wal_senders using provider tooling.`,
    });
  }

  const effectiveReplicationRole =
    role?.isSuperuser ||
    role?.hasReplication ||
    role?.memberOfCloudSQLSuperuser ||
    role?.memberOfNeonSuperuser ||
    role?.memberOfRdsReplication ||
    role?.memberOfRdsSuperuser;
  if (role && !effectiveReplicationRole) {
    findings.push({
      level: 'warning',
      code: 'role_without_replication',
      message: `The current role does not appear to have REPLICATION privilege or a known provider replication role.`,
      action: replicationRoleAction(provider),
      details: {currentUser: role.currentUser},
    });
  }
  if (role && !role.canCreateInDatabase) {
    findings.push({
      level: 'warning',
      code: 'role_without_database_create',
      message: `The current role cannot CREATE in the database. Zero may be unable to create its internal schema, publications, and metadata tables.`,
      action: `Grant CREATE on the database or pre-create the required Zero metadata/publication objects with an admin role.`,
      details: {currentUser: role.currentUser},
    });
  }

  if (settings?.maxReplicationSlots !== undefined && slots) {
    const freeSlots = settings.maxReplicationSlots - slots.totalSlots;
    if (freeSlots < 1) {
      findings.push({
        level: 'warning',
        code: 'no_free_replication_slots',
        message: `No free replication slots were detected. Zero needs at least one logical replication slot.`,
        action: `Increase max_replication_slots or remove inactive slots before starting zero-cache.`,
        details: {
          maxReplicationSlots: settings.maxReplicationSlots,
          totalSlots: slots.totalSlots,
        },
      });
    }
  }
  if (settings?.maxWalSenders !== undefined && walSenders) {
    const freeSenders = settings.maxWalSenders - walSenders.activeSenders;
    if (freeSenders < 1) {
      findings.push({
        level: 'warning',
        code: 'no_free_wal_senders',
        message: `No free WAL sender capacity was detected. Zero needs a WAL sender for logical replication.`,
        action: `Increase max_wal_senders or reduce active replication clients before starting zero-cache.`,
        details: {
          maxWalSenders: settings.maxWalSenders,
          activeSenders: walSenders.activeSenders,
        },
      });
    }
  }

  if (requestedPublications.length === 0) {
    findings.push({
      level: 'info',
      code: 'zero_managed_publication',
      message: `No application publications were configured. Zero will attempt to create an internal publication for tables in the public schema.`,
      action: `Configure ZERO_APP_PUBLICATIONS if the replicated tables are outside public or if the provider requires admin-created publications.`,
    });
  } else if (publications) {
    const existing = new Set(publications.existing.map(pub => pub.name));
    const missing = requestedPublications.filter(pub => !existing.has(pub));
    if (missing.length) {
      findings.push({
        level: 'warning',
        code: 'publications_missing',
        message: `Some configured publications do not exist.`,
        action: `Create the missing publications or correct ZERO_APP_PUBLICATIONS before starting zero-cache.`,
        details: {missing},
      });
    }
    const incomplete = publications.existing.filter(
      pub =>
        !pub.publishesInsert ||
        !pub.publishesUpdate ||
        !pub.publishesDelete ||
        !pub.publishesTruncate,
    );
    if (incomplete.length) {
      findings.push({
        level: 'warning',
        code: 'publication_missing_events',
        message: `Some configured publications do not publish every event type Zero requires.`,
        action: `Alter each publication so it publishes insert, update, delete, and truncate.`,
        details: {publications: incomplete.map(pub => pub.name)},
      });
    }
    const unreadable = publications.tablePrivileges.filter(
      table => !table.hasSchemaUsage || !table.hasSelect,
    );
    if (unreadable.length) {
      findings.push({
        level: 'warning',
        code: 'published_tables_without_select',
        message: `The current role cannot SELECT from every table in the configured publications.`,
        action: `Grant USAGE on the published schemas and SELECT on published tables to the Zero database role.`,
        details: {
          tables: unreadable.map(table => tableName(table)),
        },
      });
    }
    const rlsTables = publications.tablePrivileges.filter(
      table => table.rlsEnabled || table.rlsForced,
    );
    if (rlsTables.length && role && !role.bypassRLS && !role.isSuperuser) {
      findings.push({
        level: 'warning',
        code: 'published_tables_with_rls',
        message: `Some published tables have row-level security enabled and the current role does not bypass RLS.`,
        action: `Verify Zero should see all published rows, or grant BYPASSRLS/use a role whose policies expose the replicated data.`,
        details: {tables: rlsTables.map(table => tableName(table))},
      });
    }
  }

  return findings;
}

async function inspect<T>(
  lc: LogContext,
  label: string,
  fn: () => Promise<T>,
): Promise<T | undefined> {
  try {
    return await fn();
  } catch (e) {
    lc.warn?.(
      `Postgres replication preflight could not inspect ${label}; continuing startup`,
      e,
    );
    return undefined;
  }
}

async function inspectServerSettings(sql: PostgresDB): Promise<ServerSettings> {
  const [row] = await sql<
    {
      serverVersion: string | null;
      serverVersionNum: string | null;
      walLevel: string | null;
      maxReplicationSlots: string | null;
      maxWalSenders: string | null;
      maxSlotWalKeepSize: string | null;
      rdsLogicalReplication: string | null;
      cloudsqlLogicalDecoding: string | null;
      cloudsqlEnablePglogical: string | null;
      syncReplicationSlots: string | null;
      hotStandbyFeedback: string | null;
      inRecovery: boolean;
    }[]
  >`
    SELECT
      current_setting('server_version', true) AS "serverVersion",
      current_setting('server_version_num', true) AS "serverVersionNum",
      current_setting('wal_level', true) AS "walLevel",
      current_setting('max_replication_slots', true) AS "maxReplicationSlots",
      current_setting('max_wal_senders', true) AS "maxWalSenders",
      current_setting('max_slot_wal_keep_size', true) AS "maxSlotWalKeepSize",
      current_setting('rds.logical_replication', true) AS "rdsLogicalReplication",
      current_setting('cloudsql.logical_decoding', true) AS "cloudsqlLogicalDecoding",
      current_setting('cloudsql.enable_pglogical', true) AS "cloudsqlEnablePglogical",
      current_setting('sync_replication_slots', true) AS "syncReplicationSlots",
      current_setting('hot_standby_feedback', true) AS "hotStandbyFeedback",
      pg_is_in_recovery() AS "inRecovery"
  `;
  return {
    serverVersion: row.serverVersion ?? undefined,
    serverVersionNum: numberSetting(row.serverVersionNum),
    walLevel: row.walLevel,
    maxReplicationSlots: numberSetting(row.maxReplicationSlots),
    maxWalSenders: numberSetting(row.maxWalSenders),
    maxSlotWalKeepSize: row.maxSlotWalKeepSize,
    rdsLogicalReplication: row.rdsLogicalReplication,
    cloudsqlLogicalDecoding: row.cloudsqlLogicalDecoding,
    cloudsqlEnablePglogical: row.cloudsqlEnablePglogical,
    syncReplicationSlots: row.syncReplicationSlots,
    hotStandbyFeedback: row.hotStandbyFeedback,
    inRecovery: row.inRecovery,
  };
}

async function inspectRole(sql: PostgresDB): Promise<RoleInspection> {
  const [row] = await sql<RoleInspection[]>`
    SELECT
      current_user AS "currentUser",
      session_user AS "sessionUser",
      rol.rolsuper AS "isSuperuser",
      rol.rolreplication AS "hasReplication",
      rol.rolbypassrls AS "bypassRLS",
      has_database_privilege(current_database(), 'CREATE') AS "canCreateInDatabase",
      has_database_privilege(current_database(), 'CONNECT') AS "canConnectToDatabase",
      CASE WHEN to_regrole('cloudsqlsuperuser') IS NULL
        THEN false
        ELSE pg_has_role(current_user, 'cloudsqlsuperuser', 'member')
      END AS "memberOfCloudSQLSuperuser",
      CASE WHEN to_regrole('neon_superuser') IS NULL
        THEN false
        ELSE pg_has_role(current_user, 'neon_superuser', 'member')
      END AS "memberOfNeonSuperuser",
      CASE WHEN to_regrole('rds_replication') IS NULL
        THEN false
        ELSE pg_has_role(current_user, 'rds_replication', 'member')
      END AS "memberOfRdsReplication",
      CASE WHEN to_regrole('rds_superuser') IS NULL
        THEN false
        ELSE pg_has_role(current_user, 'rds_superuser', 'member')
      END AS "memberOfRdsSuperuser"
    FROM pg_roles rol
    WHERE rol.rolname = current_user
  `;
  return row;
}

async function inspectSlots(sql: PostgresDB): Promise<SlotInspection> {
  const [row] = await sql<SlotInspection[]>`
    SELECT
      COUNT(*)::int AS "totalSlots",
      COUNT(*) FILTER (WHERE active)::int AS "activeSlots",
      COUNT(*) FILTER (WHERE slot_type = 'logical')::int AS "logicalSlots"
    FROM pg_replication_slots
  `;
  return row;
}

async function inspectWalSenders(
  sql: PostgresDB,
): Promise<WalSenderInspection> {
  const [row] = await sql<WalSenderInspection[]>`
    SELECT COUNT(*)::int AS "activeSenders" FROM pg_stat_replication
  `;
  return row;
}

async function inspectPublications(
  sql: PostgresDB,
  requested: readonly string[],
): Promise<PublicationInspection> {
  if (requested.length === 0) {
    return {requested, existing: [], tablePrivileges: []};
  }
  const existing = await sql<PublicationRow[]>`
    SELECT
      pubname AS "name",
      pubinsert AS "publishesInsert",
      pubupdate AS "publishesUpdate",
      pubdelete AS "publishesDelete",
      pubtruncate AS "publishesTruncate"
    FROM pg_publication
    WHERE pubname IN ${sql(requested)}
    ORDER BY pubname
  `;
  const tablePrivileges = await sql<PublicationTablePrivilege[]>`
    WITH published_tables AS (
      SELECT DISTINCT schemaname, tablename
      FROM pg_publication_tables
      WHERE pubname IN ${sql(requested)}
    )
    SELECT
      pt.schemaname AS "schema",
      pt.tablename AS "table",
      has_schema_privilege(pt.schemaname, 'USAGE') AS "hasSchemaUsage",
      has_table_privilege(format('%I.%I', pt.schemaname, pt.tablename), 'SELECT') AS "hasSelect",
      cls.relrowsecurity AS "rlsEnabled",
      cls.relforcerowsecurity AS "rlsForced",
      pg_get_userbyid(cls.relowner) AS "owner",
      pg_has_role(cls.relowner, 'USAGE') AS "isOwnerOrMember"
    FROM published_tables pt
    JOIN pg_namespace ns ON ns.nspname = pt.schemaname
    JOIN pg_class cls ON cls.relnamespace = ns.oid AND cls.relname = pt.tablename
    WHERE cls.relkind IN ('r', 'p')
    ORDER BY pt.schemaname, pt.tablename
  `;
  return {requested, existing, tablePrivileges};
}

function logFindings(
  lc: LogContext,
  provider: ProviderInfo,
  endpoint: EndpointInfo,
  findings: PreflightFinding[],
) {
  const warnings = findings.filter(finding => finding.level === 'warning');
  const payload = {
    provider,
    endpoint: {
      host: endpoint.host,
      port: endpoint.port,
      database: endpoint.database,
      endpointType: endpoint.endpointType,
      reasons: endpoint.reasons,
    },
    findings,
  };
  if (warnings.length) {
    lc.warn?.(
      `Postgres replication preflight found ${warnings.length} potential setup issue(s); continuing startup`,
      payload,
    );
    return;
  }
  lc.info?.(`Postgres replication preflight found no blocking issues`, payload);
}

function providerNote(
  provider: ProviderInfo,
  endpoint: EndpointInfo,
): PreflightFinding | undefined {
  switch (provider.kind) {
    case 'aws-rds':
      return {
        level: 'info',
        code: 'provider_aws_rds',
        message: `Detected Amazon RDS for PostgreSQL. Logical replication is controlled by the rds.logical_replication parameter group setting.`,
        action: `If wal_level is not logical, enable rds.logical_replication and reboot the database.`,
      };
    case 'aws-aurora':
      return {
        level: 'info',
        code: 'provider_aws_aurora',
        message: `Detected Amazon Aurora PostgreSQL. Logical replication is controlled by the DB cluster parameter group.`,
        action: `If wal_level is not logical, enable rds.logical_replication on the cluster parameter group and reboot the writer.`,
      };
    case 'google-cloud-sql':
      return {
        level: 'info',
        code: 'provider_google_cloud_sql',
        message: `Detected Google Cloud SQL. Native logical replication requires cloudsql.logical_decoding=on.`,
        action: `If wal_level is not logical, enable cloudsql.logical_decoding and restart the instance.`,
      };
    case 'azure-flexible-server':
      return {
        level: 'info',
        code: 'provider_azure_flexible',
        message: `Detected Azure Database for PostgreSQL Flexible Server. Logical replication is controlled by server parameters and Azure firewall reachability.`,
        action: `Set wal_level=logical and verify the Zero host is allowed by Azure networking/firewall rules.`,
      };
    case 'neon':
      return {
        level: 'info',
        code: 'provider_neon',
        message: `Detected Neon. Logical replication is controlled by Neon's project feature toggle and inactive slots may be cleaned up by the platform.`,
        action: `Enable logical replication in Neon and make sure the connection is not a pooled endpoint.`,
      };
    case 'supabase':
      return {
        level: endpoint.endpointType === 'likely-pooled' ? 'warning' : 'info',
        code: 'provider_supabase',
        message: `Detected Supabase. Logical replication should use the direct database connection, not Supabase's pooler.`,
        action: `Use the direct connection string and verify the compute tier has enough replication slots and WAL senders.`,
      };
    case 'planetscale':
      return {
        level: endpoint.endpointType === 'likely-pooled' ? 'warning' : 'info',
        code: 'provider_planetscale',
        message: `Detected PlanetScale Postgres. Logical replication depends on cluster parameters, direct connections, and role/object ownership.`,
        action: `Use PlanetScale's direct Postgres endpoint and verify wal_level, slot/sender counts, failover settings, and publication/table ownership.`,
      };
    case 'render':
      return {
        level: 'warning',
        code: 'provider_render',
        message: `Detected Render Postgres. Logical replication may require Render support enablement before database-side setup can work.`,
        action: `Confirm logical replication is enabled for this Render database before relying on Zero replication.`,
      };
    case 'crunchy-bridge':
      return {
        level: 'info',
        code: 'provider_crunchy_bridge',
        message: `Detected Crunchy Bridge. Logical replication is usually enabled by default, but slot and sender capacity still matters.`,
      };
    case 'aiven':
      return {
        level: 'info',
        code: 'provider_aiven',
        message: `Detected Aiven for PostgreSQL. Logical replication is usually available, but provider service settings and slot capacity still matter.`,
      };
    case 'digitalocean':
      return {
        level: 'info',
        code: 'provider_digitalocean',
        message: `Detected DigitalOcean Managed PostgreSQL. Verify logical replication settings, public/private reachability, and slot/sender capacity.`,
      };
    case 'ibm-cloud':
      return {
        level: 'info',
        code: 'provider_ibm_cloud',
        message: `Detected IBM Cloud Databases for PostgreSQL. Some privileged logical replication operations may require provider helper functions.`,
      };
    case 'unknown':
      return undefined;
  }
}

function walLevelAction(provider: ProviderInfo): string {
  switch (provider.kind) {
    case 'aws-rds':
    case 'aws-aurora':
      return `Enable rds.logical_replication in the parameter group and reboot the database.`;
    case 'google-cloud-sql':
      return `Enable cloudsql.logical_decoding and restart the Cloud SQL instance.`;
    case 'azure-flexible-server':
      return `Set wal_level=logical in Azure server parameters and restart if required.`;
    case 'neon':
      return `Enable logical replication in the Neon project settings.`;
    case 'render':
      return `Contact Render support or use the provider workflow to enable logical replication.`;
    default:
      return `Set wal_level=logical and restart Postgres if the provider requires it.`;
  }
}

function replicationRoleAction(provider: ProviderInfo): string {
  switch (provider.kind) {
    case 'aws-rds':
    case 'aws-aurora':
      return `Grant rds_replication to the Zero database role, or use an appropriate RDS administrative role for setup.`;
    case 'google-cloud-sql':
      return `Use a Cloud SQL role with logical replication privileges, commonly a cloudsqlsuperuser setup role.`;
    case 'neon':
      return `Use a Neon role with replication capability, typically a role managed through the Neon console/CLI/API.`;
    default:
      return `Grant REPLICATION to the Zero database role or pre-create the replication slot with an admin role.`;
  }
}

function preflightKey(endpoint: EndpointInfo, shard: ShardConfig): string {
  return [
    endpoint.host ?? 'unknown-host',
    endpoint.port ?? 'default-port',
    endpoint.database ?? 'unknown-db',
    shard.appID,
    shard.shardNum,
  ].join('/');
}

function numberSetting(value: string | null): number | undefined {
  if (value === null) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function hasSetting(value: string | null | undefined): boolean {
  return value !== null && value !== undefined && value !== '';
}

function tableName({schema, table}: PublicationTablePrivilege): string {
  return `${schema}.${table}`;
}
