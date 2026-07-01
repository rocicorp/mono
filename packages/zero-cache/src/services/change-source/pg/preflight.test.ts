import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  buildReplicationPreflightFindings,
  classifyEndpoint,
  detectProvider,
  runPassiveReplicationPreflight,
  type PreflightFinding,
} from './preflight.ts';

describe('classifyEndpoint', () => {
  test('detects likely provider poolers', () => {
    expect(
      classifyEndpoint(
        'postgres://u:p@aws-0-us-east-1.pooler.supabase.com:6543/postgres',
      ),
    ).toMatchObject({
      host: 'aws-0-us-east-1.pooler.supabase.com',
      port: '6543',
      endpointType: 'likely-pooled',
    });

    expect(
      classifyEndpoint(
        'postgres://u:p@ep-example-pooler.us-east-1.neon.tech/db',
      ),
    ).toMatchObject({
      host: 'ep-example-pooler.us-east-1.neon.tech',
      endpointType: 'likely-pooled',
    });
  });

  test('classifies ordinary hosts as likely direct', () => {
    expect(
      classifyEndpoint('postgres://u:p@db.example.com:5432/postgres'),
    ).toMatchObject({
      host: 'db.example.com',
      port: '5432',
      database: 'postgres',
      endpointType: 'likely-direct',
    });
  });
});

describe('detectProvider', () => {
  test('detects common managed provider hostnames', () => {
    expect(detectProvider('ep-test.us-east-1.aws.neon.tech').kind).toBe('neon');
    expect(detectProvider('db.abcdefghijklmnop.supabase.co').kind).toBe(
      'supabase',
    );
    expect(
      detectProvider('app.cluster-abcdefghijkl.us-east-1.rds.amazonaws.com')
        .kind,
    ).toBe('aws-aurora');
    expect(
      detectProvider('app.abcdefghijkl.us-east-1.rds.amazonaws.com').kind,
    ).toBe('aws-rds');
    expect(detectProvider('db.postgres.database.azure.com').kind).toBe(
      'azure-flexible-server',
    );
  });

  test('uses visible provider settings when hostnames are not enough', () => {
    expect(
      detectProvider('127.0.0.1', {cloudsqlLogicalDecoding: 'on'}).kind,
    ).toBe('google-cloud-sql');
    expect(detectProvider('127.0.0.1', {rdsLogicalReplication: '1'}).kind).toBe(
      'aws-rds',
    );
  });
});

describe('buildReplicationPreflightFindings', () => {
  test('reports passive setup issues without throwing', () => {
    const findings = buildReplicationPreflightFindings({
      provider: {kind: 'supabase', name: 'Supabase'},
      endpoint: {
        host: 'aws-0-us-east-1.pooler.supabase.com',
        port: '6543',
        database: 'postgres',
        endpointType: 'likely-pooled',
        reasons: ['hostname looks like a pooler'],
      },
      settings: {
        walLevel: 'replica',
        maxReplicationSlots: 2,
        maxWalSenders: 1,
      },
      role: {
        currentUser: 'zero',
        sessionUser: 'zero',
        isSuperuser: false,
        hasReplication: false,
        bypassRLS: false,
        canCreateInDatabase: false,
        canConnectToDatabase: true,
        memberOfCloudSQLSuperuser: false,
        memberOfNeonSuperuser: false,
        memberOfRdsReplication: false,
        memberOfRdsSuperuser: false,
      },
      slots: {totalSlots: 2, activeSlots: 1, logicalSlots: 2},
      walSenders: {activeSenders: 1},
      publications: {
        requested: ['zero_data'],
        existing: [],
        tablePrivileges: [],
      },
      requestedPublications: ['zero_data'],
    });

    expect(codes(findings)).toEqual(
      expect.arrayContaining([
        'provider_supabase',
        'pooled_endpoint',
        'wal_level_not_logical',
        'role_without_replication',
        'role_without_database_create',
        'no_free_replication_slots',
        'no_free_wal_senders',
        'publications_missing',
      ]),
    );
    expect(findings.every(finding => finding.level !== 'warning')).toBe(false);
  });

  test('warns about RLS on published tables for non-bypass roles', () => {
    const findings = buildReplicationPreflightFindings({
      provider: {kind: 'unknown', name: 'Unknown PostgreSQL provider'},
      endpoint: {endpointType: 'likely-direct', reasons: []},
      settings: {walLevel: 'logical'},
      role: {
        currentUser: 'zero',
        sessionUser: 'zero',
        isSuperuser: false,
        hasReplication: true,
        bypassRLS: false,
        canCreateInDatabase: true,
        canConnectToDatabase: true,
        memberOfCloudSQLSuperuser: false,
        memberOfNeonSuperuser: false,
        memberOfRdsReplication: false,
        memberOfRdsSuperuser: false,
      },
      publications: {
        requested: ['zero_data'],
        existing: [
          {
            name: 'zero_data',
            publishesInsert: true,
            publishesUpdate: true,
            publishesDelete: true,
            publishesTruncate: true,
          },
        ],
        tablePrivileges: [
          {
            schema: 'public',
            table: 'issues',
            hasSchemaUsage: true,
            hasSelect: true,
            rlsEnabled: true,
            rlsForced: false,
            owner: 'postgres',
            isOwnerOrMember: false,
          },
        ],
      },
      requestedPublications: ['zero_data'],
    });

    expect(codes(findings)).toContain('published_tables_with_rls');
  });
});

describe('runPassiveReplicationPreflight', () => {
  test('logs and continues when inspection queries fail', async () => {
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    const sql = (() =>
      Promise.reject(
        new Error('database unavailable'),
      )) as unknown as PostgresDB;

    await expect(
      runPassiveReplicationPreflight(
        lc,
        sql,
        'postgres://u:p@db.example.com/postgres',
        {appID: 'zero', shardNum: 0, publications: []},
        {force: true},
      ),
    ).resolves.toBeUndefined();

    const warnings = sink.messages.filter(([level]) => level === 'warn');
    expect(warnings.length).toBeGreaterThan(0);
    expect(warnings.at(-1)?.[2][0]).toContain(
      'Postgres replication preflight found',
    );
  });
});

function codes(findings: PreflightFinding[]): string[] {
  return findings.map(finding => finding.code);
}
