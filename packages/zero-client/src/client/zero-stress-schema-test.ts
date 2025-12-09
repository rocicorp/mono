// This schema defines 220 tables across 21 business domains (3,117 columns
// total, avg 14 per table) and 809 relationships to stress test the Zero types.
// There are also ~10 recursive relationships.

import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';

// ==================== TABLE DEFINITIONS ====================

const user = table('user')
  .columns({
    workspaceId: string(),
    userId: string(),
    email: string(),
    emailVerified: boolean(),
    name: string(),
    avatarUrl: string().optional(),
    role: enumeration<'owner' | 'admin' | 'member' | 'guest'>(),
    status: enumeration<'active' | 'suspended' | 'deactivated'>(),
    phone: string().optional(),
    timezone: string(),
    locale: string(),
    twoFactorEnabled: boolean(),
    lastLoginAt: string().optional(),
    passwordHash: string(),
    metadata: json<{
      readonly preferences: {
        readonly theme?: string;
        readonly notifications?: boolean;
      };
      readonly onboarding: {
        readonly completed: boolean;
        readonly step: number;
      };
    }>(),
    activityData: json<
      | {
          readonly type: 'login';
          readonly timestamp: number;
          readonly ip: string;
          readonly device: string;
        }
      | {
          readonly type: 'logout';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'profile_update';
          readonly timestamp: number;
          readonly fields: readonly string[];
        }
      | {
          readonly type: 'password_change';
          readonly timestamp: number;
          readonly method: string;
        }
      | {
          readonly type: 'email_verification';
          readonly timestamp: number;
          readonly verified: boolean;
        }
      | {
          readonly type: 'two_factor_enable';
          readonly timestamp: number;
          readonly method: string;
        }
      | {
          readonly type: 'two_factor_disable';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'api_key_create';
          readonly timestamp: number;
          readonly keyId: string;
        }
      | {
          readonly type: 'api_key_revoke';
          readonly timestamp: number;
          readonly keyId: string;
        }
      | {
          readonly type: 'session_create';
          readonly timestamp: number;
          readonly sessionId: string;
        }
      | {
          readonly type: 'session_expire';
          readonly timestamp: number;
          readonly sessionId: string;
        }
      | {
          readonly type: 'permission_grant';
          readonly timestamp: number;
          readonly permission: string;
        }
      | {
          readonly type: 'permission_revoke';
          readonly timestamp: number;
          readonly permission: string;
        }
      | {
          readonly type: 'role_change';
          readonly timestamp: number;
          readonly from: string;
          readonly to: string;
        }
      | {
          readonly type: 'team_join';
          readonly timestamp: number;
          readonly teamId: string;
        }
      | {
          readonly type: 'team_leave';
          readonly timestamp: number;
          readonly teamId: string;
        }
      | {
          readonly type: 'workspace_join';
          readonly timestamp: number;
          readonly workspaceId: string;
        }
      | {
          readonly type: 'workspace_leave';
          readonly timestamp: number;
          readonly workspaceId: string;
        }
      | {
          readonly type: 'file_upload';
          readonly timestamp: number;
          readonly fileId: string;
          readonly size: number;
        }
      | {
          readonly type: 'file_delete';
          readonly timestamp: number;
          readonly fileId: string;
        }
      | {
          readonly type: 'comment_create';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'comment_edit';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'comment_delete';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'tag_create';
          readonly timestamp: number;
          readonly tagId: string;
        }
      | {
          readonly type: 'tag_assign';
          readonly timestamp: number;
          readonly entityId: string;
          readonly tagId: string;
        }
      | {
          readonly type: 'tag_remove';
          readonly timestamp: number;
          readonly entityId: string;
          readonly tagId: string;
        }
      | {
          readonly type: 'notification_send';
          readonly timestamp: number;
          readonly notificationId: string;
        }
      | {
          readonly type: 'notification_read';
          readonly timestamp: number;
          readonly notificationId: string;
        }
      | {
          readonly type: 'integration_connect';
          readonly timestamp: number;
          readonly provider: string;
        }
      | {
          readonly type: 'integration_disconnect';
          readonly timestamp: number;
          readonly provider: string;
        }
      | {
          readonly type: 'webhook_create';
          readonly timestamp: number;
          readonly webhookId: string;
        }
      | {
          readonly type: 'webhook_delete';
          readonly timestamp: number;
          readonly webhookId: string;
        }
      | {
          readonly type: 'export_request';
          readonly timestamp: number;
          readonly format: string;
          readonly entityType: string;
        }
      | {
          readonly type: 'import_request';
          readonly timestamp: number;
          readonly format: string;
          readonly status: string;
        }
      | {
          readonly type: 'backup_create';
          readonly timestamp: number;
          readonly backupId: string;
        }
      | {
          readonly type: 'backup_restore';
          readonly timestamp: number;
          readonly backupId: string;
        }
      | {
          readonly type: 'billing_update';
          readonly timestamp: number;
          readonly plan: string;
        }
      | {
          readonly type: 'subscription_cancel';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'subscription_renew';
          readonly timestamp: number;
          readonly expiresAt: string;
        }
      | {
          readonly type: 'payment_success';
          readonly timestamp: number;
          readonly amount: number;
          readonly currency: string;
        }
      | {
          readonly type: 'payment_failed';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'invoice_generate';
          readonly timestamp: number;
          readonly invoiceId: string;
        }
      | {
          readonly type: 'credit_add';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'credit_deduct';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'feature_enable';
          readonly timestamp: number;
          readonly feature: string;
        }
      | {
          readonly type: 'feature_disable';
          readonly timestamp: number;
          readonly feature: string;
        }
      | {
          readonly type: 'quota_exceed';
          readonly timestamp: number;
          readonly resource: string;
          readonly limit: number;
        }
      | {
          readonly type: 'quota_reset';
          readonly timestamp: number;
          readonly resource: string;
        }
      | {
          readonly type: 'security_alert';
          readonly timestamp: number;
          readonly severity: string;
          readonly message: string;
        }
      | {
          readonly type: 'compliance_check';
          readonly timestamp: number;
          readonly status: string;
          readonly details: string;
        }
    >().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'userId');

const session = table('session')
  .columns({
    workspaceId: string(),
    sessionId: string(),
    userId: string(),
    token: string(),
    expiresAt: string(),
    ipAddress: string(),
    userAgent: string(),
    deviceInfo: json<{
      readonly browser: string;
      readonly os: string;
      readonly device: string;
    }>(),
    lastActivityAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'sessionId');

const account = table('account')
  .columns({
    workspaceId: string(),
    accountId: string(),
    userId: string(),
    provider: enumeration<'google' | 'github' | 'microsoft' | 'slack'>(),
    providerAccountId: string(),
    accessToken: string().optional(),
    refreshToken: string().optional(),
    expiresAt: string().optional(),
    scope: string().optional(),
    tokenType: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'accountId');

const workspace = table('workspace')
  .columns({
    workspaceId: string(),
    name: string(),
    slug: string(),
    plan: enumeration<'free' | 'pro' | 'enterprise'>(),
    billingEmail: string(),
    settings: json<{
      readonly features: readonly string[];
      readonly limits: Record<string, number>;
    }>(),
    features: json<readonly string[]>(),
    storageUsed: number(),
    memberCount: number(),
    createdById: string(),
    status: enumeration<'active' | 'suspended' | 'trial'>(),
    trialEndsAt: string().optional(),
    eventLog: json<
      | {
          readonly kind: 'created';
          readonly by: string;
          readonly at: number;
          readonly source: string;
        }
      | {
          readonly kind: 'renamed';
          readonly from: string;
          readonly to: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'plan_upgraded';
          readonly from: string;
          readonly to: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'plan_downgraded';
          readonly from: string;
          readonly to: string;
          readonly by: string;
          readonly at: number;
          readonly reason: string;
        }
      | {
          readonly kind: 'suspended';
          readonly by: string;
          readonly at: number;
          readonly reason: string;
          readonly appealable: boolean;
        }
      | {
          readonly kind: 'reactivated';
          readonly by: string;
          readonly at: number;
          readonly previousState: string;
        }
      | {
          readonly kind: 'member_invited';
          readonly userId: string;
          readonly role: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'member_joined';
          readonly userId: string;
          readonly at: number;
          readonly invitedBy: string;
        }
      | {
          readonly kind: 'member_removed';
          readonly userId: string;
          readonly by: string;
          readonly at: number;
          readonly reason: string;
        }
      | {
          readonly kind: 'member_role_changed';
          readonly userId: string;
          readonly from: string;
          readonly to: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'team_created';
          readonly teamId: string;
          readonly name: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'team_deleted';
          readonly teamId: string;
          readonly by: string;
          readonly at: number;
          readonly memberCount: number;
        }
      | {
          readonly kind: 'integration_added';
          readonly provider: string;
          readonly by: string;
          readonly at: number;
          readonly scopes: readonly string[];
        }
      | {
          readonly kind: 'integration_removed';
          readonly provider: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'webhook_registered';
          readonly webhookId: string;
          readonly url: string;
          readonly events: readonly string[];
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'webhook_deleted';
          readonly webhookId: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'api_key_created';
          readonly keyId: string;
          readonly name: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'api_key_revoked';
          readonly keyId: string;
          readonly by: string;
          readonly at: number;
          readonly reason: string;
        }
      | {
          readonly kind: 'storage_limit_reached';
          readonly limit: number;
          readonly current: number;
          readonly at: number;
        }
      | {
          readonly kind: 'storage_limit_exceeded';
          readonly limit: number;
          readonly current: number;
          readonly at: number;
          readonly action: string;
        }
      | {
          readonly kind: 'billing_updated';
          readonly by: string;
          readonly at: number;
          readonly method: string;
        }
      | {
          readonly kind: 'payment_method_added';
          readonly type: string;
          readonly last4: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'payment_method_removed';
          readonly type: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'invoice_paid';
          readonly invoiceId: string;
          readonly amount: number;
          readonly currency: string;
          readonly at: number;
        }
      | {
          readonly kind: 'invoice_failed';
          readonly invoiceId: string;
          readonly amount: number;
          readonly reason: string;
          readonly at: number;
        }
      | {
          readonly kind: 'trial_started';
          readonly plan: string;
          readonly endsAt: string;
          readonly at: number;
        }
      | {
          readonly kind: 'trial_extended';
          readonly by: string;
          readonly days: number;
          readonly newEndsAt: string;
          readonly at: number;
        }
      | {
          readonly kind: 'trial_ended';
          readonly converted: boolean;
          readonly at: number;
        }
      | {
          readonly kind: 'trial_expired';
          readonly at: number;
          readonly gracePeriodDays: number;
        }
      | {
          readonly kind: 'feature_enabled';
          readonly feature: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'feature_disabled';
          readonly feature: string;
          readonly by: string;
          readonly at: number;
          readonly reason: string;
        }
      | {
          readonly kind: 'security_scan';
          readonly passed: boolean;
          readonly issues: number;
          readonly at: number;
        }
      | {
          readonly kind: 'compliance_audit';
          readonly standard: string;
          readonly result: string;
          readonly auditor: string;
          readonly at: number;
        }
      | {
          readonly kind: 'data_export_requested';
          readonly format: string;
          readonly entities: readonly string[];
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'data_export_completed';
          readonly exportId: string;
          readonly size: number;
          readonly at: number;
        }
      | {
          readonly kind: 'data_import_started';
          readonly source: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'data_import_completed';
          readonly importId: string;
          readonly recordsImported: number;
          readonly at: number;
        }
      | {
          readonly kind: 'backup_created';
          readonly backupId: string;
          readonly size: number;
          readonly at: number;
        }
      | {
          readonly kind: 'backup_restored';
          readonly backupId: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'settings_changed';
          readonly setting: string;
          readonly from: string;
          readonly to: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'notification_sent';
          readonly type: string;
          readonly recipients: number;
          readonly at: number;
        }
      | {
          readonly kind: 'rate_limit_exceeded';
          readonly endpoint: string;
          readonly limit: number;
          readonly at: number;
        }
      | {
          readonly kind: 'ssl_certificate_renewed';
          readonly domain: string;
          readonly expiresAt: string;
          readonly at: number;
        }
      | {
          readonly kind: 'domain_verified';
          readonly domain: string;
          readonly method: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'domain_removed';
          readonly domain: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'sso_configured';
          readonly provider: string;
          readonly by: string;
          readonly at: number;
        }
      | {
          readonly kind: 'sso_disabled';
          readonly provider: string;
          readonly by: string;
          readonly at: number;
          readonly reason: string;
        }
    >().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId');

const workspaceMember = table('workspaceMember')
  .columns({
    workspaceId: string(),
    memberId: string(),
    userId: string(),
    role: enumeration<'owner' | 'admin' | 'member' | 'guest'>(),
    permissions: json<readonly string[]>(),
    invitedById: string(),
    invitedAt: string(),
    joinedAt: string().optional(),
    lastSeenAt: string().optional(),
    status: enumeration<'invited' | 'active' | 'suspended'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'memberId');

const team = table('team')
  .columns({
    workspaceId: string(),
    teamId: string(),
    name: string(),
    description: string().optional(),
    parentTeamId: string().optional(),
    leaderId: string(),
    memberIds: json<readonly string[]>(),
    settings: json<{readonly defaultPermissions?: readonly string[]}>(),
    createdById: string(),
    archivedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'teamId');

const apiKey = table('apiKey')
  .columns({
    workspaceId: string(),
    keyId: string(),
    name: string(),
    keyHash: string(),
    userId: string(),
    permissions: json<readonly string[]>(),
    scopes: json<readonly string[]>(),
    expiresAt: string().optional(),
    lastUsedAt: string().optional(),
    ipWhitelist: json<readonly string[]>().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'keyId');

const verificationToken = table('verificationToken')
  .columns({
    workspaceId: string(),
    tokenId: string(),
    identifier: string(),
    token: string(),
    type: enumeration<'email' | 'phone' | 'password_reset' | 'invite'>(),
    expiresAt: string(),
    attempts: number(),
    metadata: json<{
      readonly source?: string;
      readonly userId?: string;
    }>().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'tokenId');

const passwordReset = table('passwordReset')
  .columns({
    workspaceId: string(),
    resetId: string(),
    userId: string(),
    token: string(),
    expiresAt: string(),
    usedAt: string().optional(),
    ipAddress: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'resetId');

const auditLog = table('auditLog')
  .columns({
    workspaceId: string(),
    logId: string(),
    userId: string(),
    action: string(),
    entityType: string(),
    entityId: string(),
    changes: json<{
      readonly before: {
        readonly [key: string]: string | number | boolean | null;
      } | null;
      readonly after: {
        readonly [key: string]: string | number | boolean | null;
      } | null;
    }>(),
    ipAddress: string(),
    userAgent: string(),
    timestamp: string(),
    metadata: json<{
      readonly resource?: string;
      readonly reason?: string;
    }>().optional(),
  })
  .primaryKey('workspaceId', 'logId');

// Auth Domain Relationships

const entityTag = table('entityTag')
  .columns({
    workspaceId: string(),
    entityType: string(),
    entityId: string(),
    tagId: string(),
    tagName: string(),
    tagColor: string().optional(),
    createdById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'entityType', 'entityId', 'tagId');

const entityAttachment = table('entityAttachment')
  .columns({
    workspaceId: string(),
    attachmentId: string(),
    entityType: string(),
    entityId: string(),
    fileName: string(),
    fileUrl: string(),
    fileSize: number(),
    mimeType: string(),
    metadata: json<{
      readonly width?: number;
      readonly height?: number;
      readonly duration?: number;
    }>().optional(),
    uploadedById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'attachmentId');

const entityComment = table('entityComment')
  .columns({
    workspaceId: string(),
    commentId: string(),
    entityType: string(),
    entityId: string(),
    content: string(),
    contentHtml: string().optional(),
    createdById: string(),
    updatedById: string().optional(),
    parentCommentId: string().optional(),
    reactions: json<Record<string, readonly string[]>>().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'commentId');

const customFieldDefinition = table('customFieldDefinition')
  .columns({
    workspaceId: string(),
    fieldId: string(),
    entityType: string(),
    fieldName: string(),
    fieldType: enumeration<
      'text' | 'number' | 'boolean' | 'date' | 'select' | 'multiselect'
    >(),
    validation: json<{
      readonly required?: boolean;
      readonly min?: number;
      readonly max?: number;
      readonly options?: readonly string[];
    }>().optional(),
    defaultValue: json<string | number | boolean | null>().optional(),
    displayOrder: number(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'fieldId');

const customFieldValue = table('customFieldValue')
  .columns({
    workspaceId: string(),
    entityType: string(),
    entityId: string(),
    fieldId: string(),
    value: json<string | number | boolean | readonly string[] | null>(),
    updatedById: string(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'entityType', 'entityId', 'fieldId');

const webhook = table('webhook')
  .columns({
    workspaceId: string(),
    webhookId: string(),
    name: string(),
    url: string(),
    events: json<readonly string[]>(),
    secret: string(),
    status: enumeration<'active' | 'inactive' | 'failed'>(),
    headers: json<Record<string, string>>().optional(),
    retryPolicy: json<{
      readonly maxRetries: number;
      readonly backoff: string;
    }>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'webhookId');

const webhookDelivery = table('webhookDelivery')
  .columns({
    workspaceId: string(),
    deliveryId: string(),
    webhookId: string(),
    event: string(),
    payload: json<{
      readonly event: string;
      readonly data: readonly string[];
    }>(),
    responseStatus: number().optional(),
    responseBody: string().optional(),
    status: enumeration<'pending' | 'success' | 'failed'>(),
    attemptCount: number(),
    nextRetryAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'deliveryId');

const rateLimit = table('rateLimit')
  .columns({
    workspaceId: string(),
    limitId: string(),
    entityType: string(),
    entityId: string(),
    action: string(),
    count: number(),
    resetAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'limitId');

const featureFlag = table('featureFlag')
  .columns({
    workspaceId: string(),
    flagId: string(),
    flagKey: string(),
    enabled: boolean(),
    rolloutPercentage: number(),
    targetUserIds: json<readonly string[]>().optional(),
    targetSegments: json<readonly string[]>().optional(),
    metadata: json<{
      readonly description?: string;
      readonly tags?: readonly string[];
    }>().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'flagId');

const integration = table('integration')
  .columns({
    workspaceId: string(),
    integrationId: string(),
    provider: string(),
    credentials: json<{
      readonly apiKey?: string;
      readonly secret?: string;
    }>(),
    config: json<{
      readonly webhookUrl?: string;
      readonly syncFrequency?: number;
    }>(),
    status: enumeration<'connected' | 'disconnected' | 'error'>(),
    lastSyncAt: string().optional(),
    userId: string(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'integrationId');

// Universal/Junction Domain Relationships

const emailCampaign = table('emailCampaign')
  .columns({
    workspaceId: string(),
    campaignId: string(),
    name: string(),
    subject: string(),
    previewText: string().optional(),
    fromName: string(),
    fromEmail: string(),
    replyToEmail: string().optional(),
    sendAt: string().optional(),
    status: enumeration<
      'draft' | 'scheduled' | 'sending' | 'sent' | 'paused'
    >(),
    abTestConfig: json<{
      readonly enabled: boolean;
      readonly variants: readonly {
        readonly subject: string;
        readonly weight: number;
      }[];
    }>().optional(),
    segmentIds: json<readonly string[]>(),
    stats: json<{
      readonly sent: number;
      readonly delivered: number;
      readonly opened: number;
      readonly clicked: number;
      readonly bounced: number;
      readonly unsubscribed: number;
    }>(),
    contentHtml: string(),
    contentPlain: string(),
    contentMjml: string().optional(),
    contentMdast: json<
      readonly {
        readonly type: string;
        readonly children?: readonly {
          readonly type: string;
          readonly value: string;
        }[];
      }[]
    >().optional(),
    templateId: string().optional(),
    goalMetrics: json<{
      readonly targetOpenRate?: number;
      readonly targetClickRate?: number;
      readonly targetConversions?: number;
    }>().optional(),
    utmParameters: json<{
      readonly source: string;
      readonly medium: string;
      readonly campaign: string;
      readonly term?: string;
      readonly content?: string;
    }>(),
    personalizationRules:
      json<
        readonly {readonly field: string; readonly fallback: string}[]
      >().optional(),
    sendWindowStart: string().optional(),
    sendWindowEnd: string().optional(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'campaignId');

const emailTemplate = table('emailTemplate')
  .columns({
    workspaceId: string(),
    templateId: string(),
    name: string(),
    subject: string(),
    htmlContent: string(),
    plaintextContent: string(),
    mjmlSource: string().optional(),
    previewImageUrl: string().optional(),
    variables: json<
      readonly {
        readonly name: string;
        readonly defaultValue: string;
        readonly type: string;
        readonly required: boolean;
      }[]
    >(),
    category: string().optional(),
    isShared: boolean(),
    metadata: json<{
      readonly lastUsed?: string;
      readonly useCount?: number;
    }>().optional(),
    thumbnailUrl: string().optional(),
    contentBlocks: json<
      readonly {
        readonly id: string;
        readonly type: string;
        readonly content: {readonly [key: string]: string};
      }[]
    >(),
    styles: json<{
      readonly fonts: readonly string[];
      readonly colors: readonly string[];
      readonly spacing: {readonly [key: string]: number};
    }>().optional(),
    responsive: boolean(),
    darkModeSupport: boolean(),
    ampSupport: boolean(),
    tags: json<readonly string[]>(),
    versionHistory: json<
      readonly {
        readonly version: number;
        readonly updatedAt: string;
        readonly updatedById: string;
      }[]
    >(),
    approvalStatus: enumeration<
      'draft' | 'pending_review' | 'approved' | 'rejected'
    >(),
    approvedById: string().optional(),
    approvedAt: string().optional(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'templateId');

const emailSend = table('emailSend')
  .columns({
    workspaceId: string(),
    sendId: string(),
    campaignId: string(),
    subscriberId: string(),
    recipientEmail: string(),
    recipientName: string().optional(),
    deliveryStatus: enumeration<
      'queued' | 'sent' | 'delivered' | 'bounced' | 'failed'
    >(),
    bounceType: enumeration<'hard' | 'soft' | 'complaint'>().optional(),
    bounceReason: string().optional(),
    openedAt: string().optional(),
    openCount: number(),
    clickedAt: string().optional(),
    clickCount: number(),
    unsubscribedAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'sendId');

const subscriberList = table('subscriberList')
  .columns({
    workspaceId: string(),
    listId: string(),
    name: string(),
    description: string().optional(),
    doubleOptIn: boolean(),
    subscriberCount: number(),
    activeCount: number(),
    unsubscribeCount: number(),
    customFields:
      json<readonly {readonly name: string; readonly type: string}[]>(),
    growthStats: json<{
      readonly daily: readonly number[];
      readonly weekly: readonly number[];
    }>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'listId');

const subscriber = table('subscriber')
  .columns({
    workspaceId: string(),
    subscriberId: string(),
    email: string(),
    status: enumeration<
      'subscribed' | 'unsubscribed' | 'bounced' | 'pending'
    >(),
    listIds: json<readonly string[]>(),
    customFields: json<{
      readonly [key: string]: string | number | boolean;
    }>(),
    engagementScore: number(),
    tags: json<readonly string[]>(),
    subscribedAt: string(),
    unsubscribedAt: string().optional(),
    lastActivityAt: string().optional(),
    ipAddress: string().optional(),
    source: string().optional(),
    firstName: string().optional(),
    lastName: string().optional(),
    phone: string().optional(),
    timezone: string().optional(),
    locale: string().optional(),
    preferences: json<{
      readonly emailFrequency: string;
      readonly topics: readonly string[];
      readonly format: string;
    }>().optional(),
    bounceHistory: json<
      readonly {
        readonly date: string;
        readonly type: string;
        readonly reason: string;
      }[]
    >(),
    emailClients: json<readonly string[]>(),
    deviceTypes: json<readonly string[]>(),
    lastOpenDate: string().optional(),
    lastClickDate: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'subscriberId');

const automationWorkflow = table('automationWorkflow')
  .columns({
    workspaceId: string(),
    workflowId: string(),
    name: string(),
    description: string().optional(),
    trigger: json<{
      readonly type: string;
      readonly conditions: readonly {
        readonly field: string;
        readonly operator: string;
        readonly value: string;
      }[];
    }>(),
    steps: json<
      readonly {
        readonly id: string;
        readonly type: string;
        readonly config: {
          readonly [key: string]: string | number | boolean;
        };
      }[]
    >(),
    isActive: boolean(),
    performanceMetrics: json<{
      readonly enrolled: number;
      readonly completed: number;
      readonly goalReached: number;
    }>(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'workflowId');

const emailLink = table('emailLink')
  .columns({
    workspaceId: string(),
    linkId: string(),
    sendId: string(),
    originalUrl: string(),
    trackedUrl: string(),
    clickCount: number(),
    uniqueClicks: number(),
    utmParams: json<{
      readonly source?: string;
      readonly medium?: string;
      readonly campaign?: string;
      readonly term?: string;
      readonly content?: string;
    }>().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'linkId', 'sendId');

const unsubscribeEvent = table('unsubscribeEvent')
  .columns({
    workspaceId: string(),
    eventId: string(),
    subscriberId: string(),
    reason: enumeration<
      'not_interested' | 'too_frequent' | 'never_subscribed' | 'other'
    >(),
    feedbackText: string().optional(),
    campaignId: string().optional(),
    ipAddress: string(),
    userAgent: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'eventId');

const emailAttachment = table('emailAttachment')
  .columns({
    workspaceId: string(),
    attachmentId: string(),
    fileName: string(),
    fileUrl: string(),
    s3Key: string(),
    mimeType: string(),
    fileSize: number(),
    virusScanStatus: enumeration<'pending' | 'clean' | 'infected'>(),
    virusScanAt: string().optional(),
    uploadedById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'attachmentId');

const spamComplaint = table('spamComplaint')
  .columns({
    workspaceId: string(),
    complaintId: string(),
    sendId: string(),
    subscriberId: string(),
    ispFeedback: json<{
      readonly isp: string;
      readonly feedbackType: string;
      readonly originalHeaders: readonly string[];
    }>(),
    complaintType: enumeration<
      'abuse' | 'fraud' | 'not-spam' | 'virus' | 'other'
    >(),
    resolvedStatus: boolean(),
    resolvedAt: string().optional(),
    resolvedById: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'complaintId');

// Email Marketing Domain Relationships

const supportTicket = table('supportTicket')
  .columns({
    workspaceId: string(),
    ticketId: string(),
    subject: string(),
    descriptionHtml: string(),
    descriptionMarkdown: string(),
    descriptionPlain: string(),
    priority: enumeration<'low' | 'medium' | 'high' | 'urgent'>(),
    status: enumeration<'new' | 'open' | 'pending' | 'solved' | 'closed'>(),
    channel: enumeration<'email' | 'chat' | 'phone' | 'web' | 'api'>(),
    slaResponseDue: string().optional(),
    slaResolutionDue: string().optional(),
    slaBreached: boolean(),
    orderId: string().optional(),
    customerId: string().optional(),
    assignedToId: string().optional(),
    teamId: string().optional(),
    tags: json<readonly string[]>(),
    customerEmail: string(),
    customerPhone: string().optional(),
    customerIpAddress: string().optional(),
    sentiment: enumeration<'negative' | 'neutral' | 'positive'>().optional(),
    firstResponseAt: string().optional(),
    resolvedAt: string().optional(),
    closedAt: string().optional(),
    reopenCount: number(),
    messageCount: number(),
    satisfactionRating: number().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
    activityData: json<
      | {
          readonly type: 'login';
          readonly timestamp: number;
          readonly ip: string;
          readonly device: string;
        }
      | {
          readonly type: 'logout';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'profile_update';
          readonly timestamp: number;
          readonly fields: readonly string[];
        }
      | {
          readonly type: 'password_change';
          readonly timestamp: number;
          readonly method: string;
        }
      | {
          readonly type: 'email_verification';
          readonly timestamp: number;
          readonly verified: boolean;
        }
      | {
          readonly type: 'two_factor_enable';
          readonly timestamp: number;
          readonly method: string;
        }
      | {
          readonly type: 'two_factor_disable';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'api_key_create';
          readonly timestamp: number;
          readonly keyId: string;
        }
      | {
          readonly type: 'api_key_revoke';
          readonly timestamp: number;
          readonly keyId: string;
        }
      | {
          readonly type: 'session_create';
          readonly timestamp: number;
          readonly sessionId: string;
        }
      | {
          readonly type: 'session_expire';
          readonly timestamp: number;
          readonly sessionId: string;
        }
      | {
          readonly type: 'permission_grant';
          readonly timestamp: number;
          readonly permission: string;
        }
      | {
          readonly type: 'permission_revoke';
          readonly timestamp: number;
          readonly permission: string;
        }
      | {
          readonly type: 'role_change';
          readonly timestamp: number;
          readonly from: string;
          readonly to: string;
        }
      | {
          readonly type: 'team_join';
          readonly timestamp: number;
          readonly teamId: string;
        }
      | {
          readonly type: 'team_leave';
          readonly timestamp: number;
          readonly teamId: string;
        }
      | {
          readonly type: 'workspace_join';
          readonly timestamp: number;
          readonly workspaceId: string;
        }
      | {
          readonly type: 'workspace_leave';
          readonly timestamp: number;
          readonly workspaceId: string;
        }
      | {
          readonly type: 'file_upload';
          readonly timestamp: number;
          readonly fileId: string;
          readonly size: number;
        }
      | {
          readonly type: 'file_delete';
          readonly timestamp: number;
          readonly fileId: string;
        }
      | {
          readonly type: 'comment_create';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'comment_edit';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'subscription_cancel';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'subscription_renew';
          readonly timestamp: number;
          readonly expiresAt: string;
        }
      | {
          readonly type: 'payment_success';
          readonly timestamp: number;
          readonly amount: number;
          readonly currency: string;
        }
      | {
          readonly type: 'payment_failed';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'invoice_generate';
          readonly timestamp: number;
          readonly invoiceId: string;
        }
      | {
          readonly type: 'credit_add';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'credit_deduct';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'feature_enable';
          readonly timestamp: number;
          readonly feature: string;
        }
      | {
          readonly type: 'feature_disable';
          readonly timestamp: number;
          readonly feature: string;
        }
      | {
          readonly type: 'quota_exceed';
          readonly timestamp: number;
          readonly resource: string;
          readonly limit: number;
        }
      | {
          readonly type: 'quota_reset';
          readonly timestamp: number;
          readonly resource: string;
        }
      | {
          readonly type: 'security_alert';
          readonly timestamp: number;
          readonly severity: string;
          readonly message: string;
        }
      | {
          readonly type: 'compliance_check';
          readonly timestamp: number;
          readonly status: string;
          readonly details: string;
        }
    >().optional(),
  })
  .primaryKey('workspaceId', 'ticketId');

const ticketMessage = table('ticketMessage')
  .columns({
    workspaceId: string(),
    messageId: string(),
    ticketId: string(),
    senderType: enumeration<'customer' | 'agent' | 'system'>(),
    senderUserId: string().optional(),
    senderEmail: string(),
    senderName: string(),
    bodyHtml: string(),
    bodyPlain: string(),
    attachmentIds: json<readonly string[]>(),
    isInternal: boolean(),
    sentAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'messageId');

const knowledgeBaseArticle = table('knowledgeBaseArticle')
  .columns({
    workspaceId: string(),
    articleId: string(),
    title: string(),
    contentHtml: string(),
    contentMarkdown: string(),
    contentSearchText: string(),
    excerpt: string().optional(),
    categoryId: string(),
    authorId: string(),
    viewCount: number(),
    helpfulVotes: number(),
    unhelpfulVotes: number(),
    status: enumeration<'draft' | 'published' | 'archived'>(),
    publishedAt: string().optional(),
    tags: json<readonly string[]>(),
    relatedArticleIds: json<readonly string[]>(),
    seoTitle: string().optional(),
    seoDescription: string().optional(),
    slug: string(),
    featuredImageUrl: string().optional(),
    videoTutorialUrl: string().optional(),
    attachmentUrls: json<readonly string[]>(),
    averageReadTime: number(),
    searchKeywords: json<readonly string[]>(),
    lastReviewedAt: string().optional(),
    reviewedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'articleId');

const slaPolicy = table('slaPolicy')
  .columns({
    workspaceId: string(),
    policyId: string(),
    name: string(),
    description: string().optional(),
    responseTargetMinutes: number(),
    resolutionTargetMinutes: number(),
    businessHours: json<{
      readonly timezone: string;
      readonly days: readonly {
        readonly day: string;
        readonly start: string;
        readonly end: string;
      }[];
    }>(),
    escalationRules: json<
      readonly {
        readonly triggerMinutes: number;
        readonly action: string;
        readonly targetUserId?: string;
      }[]
    >(),
    priorityMappings: json<{
      readonly [priority: string]: {
        readonly response: number;
        readonly resolution: number;
      };
    }>(),
    isActive: boolean(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'policyId');

const cannedResponse = table('cannedResponse')
  .columns({
    workspaceId: string(),
    responseId: string(),
    name: string(),
    shortcutKey: string().optional(),
    contentHtml: string(),
    contentPlain: string(),
    variables: json<
      readonly {
        readonly name: string;
        readonly placeholder: string;
        readonly defaultValue?: string;
      }[]
    >(),
    category: string().optional(),
    usageCount: number(),
    isShared: boolean(),
    tags: json<readonly string[]>(),
    language: string(),
    tone: enumeration<'formal' | 'casual' | 'empathetic' | 'technical'>(),
    targetChannels: json<readonly string[]>(),
    attachmentUrls: json<readonly string[]>().optional(),
    lastUsedAt: string().optional(),
    lastUsedById: string().optional(),
    averageSatisfactionRating: number().optional(),
    teamIds: json<readonly string[]>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'responseId');

const ticketTag = table('ticketTag')
  .columns({
    workspaceId: string(),
    tagId: string(),
    tagName: string(),
    color: string(),
    category: string().optional(),
    autoApplyRules: json<
      readonly {
        readonly condition: string;
        readonly value: string;
      }[]
    >().optional(),
    ticketCount: number(),
    createdById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'tagId');

const satisfactionSurvey = table('satisfactionSurvey')
  .columns({
    workspaceId: string(),
    surveyId: string(),
    ticketId: string(),
    customerId: string(),
    rating: number(),
    comment: string().optional(),
    sentimentScore: number().optional(),
    surveyedAt: string(),
    respondedAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'surveyId');

const agentAssignment = table('agentAssignment')
  .columns({
    workspaceId: string(),
    assignmentId: string(),
    ticketId: string(),
    agentUserId: string(),
    assignedAt: string(),
    assignedById: string(),
    workloadScore: number(),
    teamId: string().optional(),
    createdAt: number(),
    activityData: json<
      | {
          readonly type: 'login';
          readonly timestamp: number;
          readonly ip: string;
          readonly device: string;
        }
      | {
          readonly type: 'logout';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'profile_update';
          readonly timestamp: number;
          readonly fields: readonly string[];
        }
      | {
          readonly type: 'password_change';
          readonly timestamp: number;
          readonly method: string;
        }
      | {
          readonly type: 'email_verification';
          readonly timestamp: number;
          readonly verified: boolean;
        }
      | {
          readonly type: 'two_factor_enable';
          readonly timestamp: number;
          readonly method: string;
        }
      | {
          readonly type: 'two_factor_disable';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'api_key_create';
          readonly timestamp: number;
          readonly keyId: string;
        }
      | {
          readonly type: 'api_key_revoke';
          readonly timestamp: number;
          readonly keyId: string;
        }
      | {
          readonly type: 'session_create';
          readonly timestamp: number;
          readonly sessionId: string;
        }
      | {
          readonly type: 'session_expire';
          readonly timestamp: number;
          readonly sessionId: string;
        }
      | {
          readonly type: 'permission_grant';
          readonly timestamp: number;
          readonly permission: string;
        }
      | {
          readonly type: 'permission_revoke';
          readonly timestamp: number;
          readonly permission: string;
        }
      | {
          readonly type: 'role_change';
          readonly timestamp: number;
          readonly from: string;
          readonly to: string;
        }
      | {
          readonly type: 'team_join';
          readonly timestamp: number;
          readonly teamId: string;
        }
      | {
          readonly type: 'team_leave';
          readonly timestamp: number;
          readonly teamId: string;
        }
      | {
          readonly type: 'workspace_join';
          readonly timestamp: number;
          readonly workspaceId: string;
        }
      | {
          readonly type: 'workspace_leave';
          readonly timestamp: number;
          readonly workspaceId: string;
        }
      | {
          readonly type: 'file_upload';
          readonly timestamp: number;
          readonly fileId: string;
          readonly size: number;
        }
      | {
          readonly type: 'file_delete';
          readonly timestamp: number;
          readonly fileId: string;
        }
      | {
          readonly type: 'comment_create';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'comment_edit';
          readonly timestamp: number;
          readonly commentId: string;
        }
      | {
          readonly type: 'subscription_cancel';
          readonly timestamp: number;
          readonly reason: string;
        }
      | {
          readonly type: 'subscription_renew';
          readonly timestamp: number;
          readonly expiresAt: string;
        }
      | {
          readonly type: 'payment_success';
          readonly timestamp: number;
          readonly amount: number;
          readonly currency: string;
        }
      | {
          readonly type: 'payment_failed';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'invoice_generate';
          readonly timestamp: number;
          readonly invoiceId: string;
        }
      | {
          readonly type: 'credit_add';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'credit_deduct';
          readonly timestamp: number;
          readonly amount: number;
          readonly reason: string;
        }
      | {
          readonly type: 'feature_enable';
          readonly timestamp: number;
          readonly feature: string;
        }
      | {
          readonly type: 'feature_disable';
          readonly timestamp: number;
          readonly feature: string;
        }
      | {
          readonly type: 'quota_exceed';
          readonly timestamp: number;
          readonly resource: string;
          readonly limit: number;
        }
      | {
          readonly type: 'quota_reset';
          readonly timestamp: number;
          readonly resource: string;
        }
      | {
          readonly type: 'security_alert';
          readonly timestamp: number;
          readonly severity: string;
          readonly message: string;
        }
      | {
          readonly type: 'compliance_check';
          readonly timestamp: number;
          readonly status: string;
          readonly details: string;
        }
    >().optional(),
  })
  .primaryKey('workspaceId', 'assignmentId');

const ticketEscalation = table('ticketEscalation')
  .columns({
    workspaceId: string(),
    escalationId: string(),
    ticketId: string(),
    reason: string(),
    escalatedToUserId: string(),
    escalatedFromUserId: string().optional(),
    resolutionNotes: string().optional(),
    resolvedAt: string().optional(),
    escalatedAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'escalationId');

const ticketMerge = table('ticketMerge')
  .columns({
    workspaceId: string(),
    mergeId: string(),
    primaryTicketId: string(),
    mergedTicketIds: json<readonly string[]>(),
    mergeReason: string(),
    preservedMessages: json<
      readonly {
        readonly ticketId: string;
        readonly messageIds: readonly string[];
      }[]
    >(),
    mergedById: string(),
    mergedAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'mergeId');

// Customer Support Domain Relationships

const product = table('product')
  .columns({
    workspaceId: string(),
    productId: string(),
    sku: string(),
    name: string(),
    descriptionHtml: string(),
    descriptionPlain: string(),
    descriptionStructured: json<
      readonly {
        readonly type: string;
        readonly content: string;
      }[]
    >(),
    variants: json<
      readonly {
        readonly id: string;
        readonly options: {readonly [key: string]: string};
      }[]
    >(),
    seoTitle: string().optional(),
    seoDescription: string().optional(),
    seoKeywords: json<readonly string[]>().optional(),
    inventoryCount: number(),
    lowStockThreshold: number().optional(),
    status: enumeration<'draft' | 'active' | 'archived'>(),
    categoryIds: json<readonly string[]>(),
    tags: json<readonly string[]>(),
    basePrice: number(),
    compareAtPrice: number().optional(),
    costPerUnit: number().optional(),
    images: json<
      readonly {
        readonly url: string;
        readonly alt: string;
        readonly order: number;
      }[]
    >(),
    vendor: string().optional(),
    productType: string(),
    reviewCount: number(),
    averageRating: number(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'productId');

const productVariant = table('productVariant')
  .columns({
    workspaceId: string(),
    variantId: string(),
    productId: string(),
    optionValues: json<{readonly [key: string]: string}>(),
    price: number(),
    compareAtPrice: number().optional(),
    costPerItem: number().optional(),
    inventoryQuantity: number(),
    barcode: string().optional(),
    weight: number().optional(),
    weightUnit: string().optional(),
    requiresShipping: boolean(),
    sku: string(),
    imageUrl: string().optional(),
    dimensions: json<{
      readonly length: number;
      readonly width: number;
      readonly height: number;
      readonly unit: string;
    }>().optional(),
    taxable: boolean(),
    taxCode: string().optional(),
    fulfillmentService: string(),
    inventoryPolicy: enumeration<'deny' | 'continue'>(),
    position: number(),
    hsCode: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'variantId');

const order = table('order')
  .columns({
    workspaceId: string(),
    orderId: string(),
    orderNumber: string(),
    customerEmail: string(),
    customerPhone: string().optional(),
    shippingAddress: json<{
      readonly name: string;
      readonly address1: string;
      readonly address2?: string;
      readonly city: string;
      readonly province: string;
      readonly zip: string;
      readonly country: string;
    }>(),
    billingAddress: json<{
      readonly name: string;
      readonly address1: string;
      readonly address2?: string;
      readonly city: string;
      readonly province: string;
      readonly zip: string;
      readonly country: string;
    }>(),
    lineItems: json<
      readonly {
        readonly productId: string;
        readonly quantity: number;
        readonly price: number;
      }[]
    >(),
    subtotal: number(),
    taxTotal: number(),
    shippingTotal: number(),
    discountTotal: number(),
    total: number(),
    currency: string(),
    paymentStatus: enumeration<
      'pending' | 'authorized' | 'paid' | 'refunded' | 'failed'
    >(),
    fulfillmentStatus: enumeration<
      'unfulfilled' | 'partial' | 'fulfilled' | 'returned'
    >(),
    notes: string().optional(),
    customerId: string().optional(),
    customerIpAddress: string().optional(),
    referringUrl: string().optional(),
    landingPageUrl: string().optional(),
    browserInfo: json<{
      readonly userAgent: string;
      readonly acceptLanguage: string;
    }>().optional(),
    trackingNumbers:
      json<readonly {readonly carrier: string; readonly number: string}[]>(),
    estimatedDeliveryDate: string().optional(),
    createdById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'orderId');

const orderLineItem = table('orderLineItem')
  .columns({
    workspaceId: string(),
    lineItemId: string(),
    orderId: string(),
    productId: string(),
    variantId: string().optional(),
    quantity: number(),
    price: number(),
    discounts:
      json<readonly {readonly code: string; readonly amount: number}[]>(),
    taxAmount: number(),
    taxRate: number(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'lineItemId');

const shoppingCart = table('shoppingCart')
  .columns({
    workspaceId: string(),
    cartId: string(),
    sessionId: string().optional(),
    customerId: string().optional(),
    lineItems: json<
      readonly {
        readonly productId: string;
        readonly variantId: string;
        readonly quantity: number;
      }[]
    >(),
    appliedDiscounts:
      json<readonly {readonly code: string; readonly amount: number}[]>(),
    abandonedAt: string().optional(),
    abandonedEmailSent: boolean(),
    currency: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'cartId');

const productReview = table('productReview')
  .columns({
    workspaceId: string(),
    reviewId: string(),
    productId: string(),
    rating: number(),
    title: string(),
    content: string(),
    reviewerName: string(),
    reviewerEmail: string(),
    verifiedPurchase: boolean(),
    moderationStatus: enumeration<
      'pending' | 'approved' | 'rejected' | 'spam'
    >(),
    helpfulCount: number(),
    unhelpfulCount: number(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'reviewId');

const inventoryAdjustment = table('inventoryAdjustment')
  .columns({
    workspaceId: string(),
    adjustmentId: string(),
    productId: string(),
    variantId: string().optional(),
    quantityDelta: number(),
    reason: enumeration<
      'recount' | 'damage' | 'theft' | 'correction' | 'return'
    >(),
    referenceNumber: string().optional(),
    locationId: string().optional(),
    notes: string().optional(),
    createdById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'adjustmentId');

const discountCode = table('discountCode')
  .columns({
    workspaceId: string(),
    codeId: string(),
    code: string(),
    discountType: enumeration<
      'percentage' | 'fixed_amount' | 'free_shipping'
    >(),
    value: number(),
    minPurchaseAmount: number().optional(),
    usageLimitTotal: number().optional(),
    usageLimitPerCustomer: number().optional(),
    validFrom: string(),
    validUntil: string().optional(),
    appliedCount: number(),
    isActive: boolean(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'codeId');

const shippingZone = table('shippingZone')
  .columns({
    workspaceId: string(),
    zoneId: string(),
    zoneName: string(),
    countries: json<readonly string[]>(),
    regions: json<
      readonly {
        readonly country: string;
        readonly provinces: readonly string[];
      }[]
    >(),
    rateTables: json<
      readonly {
        readonly minWeight: number;
        readonly maxWeight: number;
        readonly rate: number;
      }[]
    >(),
    carrierMapping: json<{readonly [carrier: string]: string}>(),
    deliveryEstimateDays: number().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'zoneId');

const paymentTransaction = table('paymentTransaction')
  .columns({
    workspaceId: string(),
    transactionId: string(),
    orderId: string(),
    gateway: string(),
    amount: number(),
    currency: string(),
    status: enumeration<
      'pending' | 'processing' | 'success' | 'failed' | 'refunded'
    >(),
    processorResponse: json<{
      readonly code: string;
      readonly message: string;
      readonly raw: {readonly [key: string]: string | number};
    }>(),
    fraudScore: number().optional(),
    fraudReview: boolean(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'transactionId');

// E-commerce Domain Relationships

const cmsArticle = table('cmsArticle')
  .columns({
    workspaceId: string(),
    articleId: string(),
    title: string(),
    slug: string(),
    contentHtml: string(),
    contentMarkdown: string(),
    contentPortableText:
      json<readonly {readonly type: string; readonly content: string}[]>(),
    excerpt: string().optional(),
    featuredImage: string().optional(),
    seoTitle: string().optional(),
    seoDescription: string().optional(),
    seoKeywords: json<readonly string[]>().optional(),
    canonicalUrl: string().optional(),
    status: enumeration<'draft' | 'published' | 'scheduled' | 'archived'>(),
    publishedAt: string().optional(),
    scheduledFor: string().optional(),
    viewCount: number(),
    authorId: string(),
    categoryIds: json<readonly string[]>(),
    tags: json<readonly string[]>(),
    relatedArticles: json<readonly string[]>(),
    readTimeMinutes: number(),
    socialShareCounts: json<{
      readonly facebook: number;
      readonly twitter: number;
      readonly linkedin: number;
    }>(),
    lastModifiedById: string().optional(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'articleId');

const cmsPage = table('cmsPage')
  .columns({
    workspaceId: string(),
    pageId: string(),
    path: string(),
    template: string(),
    contentBlocks: json<
      readonly {
        readonly id: string;
        readonly type: string;
        readonly content: {
          readonly [key: string]: string | number | boolean;
        };
        readonly order: number;
      }[]
    >(),
    metaTitle: string().optional(),
    metaDescription: string().optional(),
    metaTags:
      json<
        readonly {readonly name: string; readonly content: string}[]
      >().optional(),
    publishedVersion: number(),
    draftChanges: json<{
      readonly [key: string]: string | number | boolean;
    }>().optional(),
    canonicalUrl: string().optional(),
    status: enumeration<'draft' | 'published'>(),
    locale: string(),
    translationIds: json<{readonly [locale: string]: string}>().optional(),
    parentPageId: string().optional(),
    navigationTitle: string().optional(),
    accessControl: json<{
      readonly requiresAuth: boolean;
      readonly allowedRoles: readonly string[];
    }>(),
    customCss: string().optional(),
    customJs: string().optional(),
    viewCount: number(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'pageId');

const mediaAsset = table('mediaAsset')
  .columns({
    workspaceId: string(),
    assetId: string(),
    fileName: string(),
    fileUrl: string(),
    cdnUrls: json<{readonly [size: string]: string}>(),
    mimeType: string(),
    fileSize: number(),
    metadata: json<{
      readonly width?: number;
      readonly height?: number;
      readonly duration?: number;
      readonly exif?: {readonly [key: string]: string | number};
    }>(),
    altText: string().optional(),
    focalPoint: json<{readonly x: number; readonly y: number}>().optional(),
    transformations: json<
      readonly {
        readonly name: string;
        readonly params: {readonly [key: string]: string | number};
      }[]
    >().optional(),
    tags: json<readonly string[]>(),
    thumbnailUrl: string().optional(),
    category: string().optional(),
    description: string().optional(),
    copyright: string().optional(),
    license: string().optional(),
    colorPalette: json<readonly string[]>().optional(),
    usageCount: number(),
    lastUsedAt: string().optional(),
    storageProvider: string(),
    uploadedById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'assetId');

const contentRevision = table('contentRevision')
  .columns({
    workspaceId: string(),
    revisionId: string(),
    entityType: string(),
    entityId: string(),
    revisionNumber: number(),
    contentSnapshot: json<{
      readonly [key: string]: string | number | boolean;
    }>(),
    changedFields: json<readonly string[]>(),
    authorId: string(),
    changeSummary: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'revisionId');

const taxonomyTerm = table('taxonomyTerm')
  .columns({
    workspaceId: string(),
    termId: string(),
    vocabulary: string(),
    termName: string(),
    slug: string(),
    parentTermId: string().optional(),
    hierarchyPath: string(),
    description: string().optional(),
    termMetadata: json<{readonly [key: string]: string}>().optional(),
    sortOrder: number(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'termId');

const contentBlock = table('contentBlock')
  .columns({
    workspaceId: string(),
    blockId: string(),
    blockType: string(),
    configuration: json<{
      readonly [key: string]: string | number | boolean;
    }>(),
    contentFields: json<{readonly [key: string]: string}>(),
    placementRules: json<{
      readonly pages?: readonly string[];
      readonly templates?: readonly string[];
      readonly conditions?: readonly {
        readonly field: string;
        readonly value: string;
      }[];
    }>().optional(),
    abTestVariants: json<
      readonly {
        readonly id: string;
        readonly content: {readonly [key: string]: string};
        readonly weight: number;
      }[]
    >().optional(),
    isActive: boolean(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'blockId');

const cmsMenu = table('cmsMenu')
  .columns({
    workspaceId: string(),
    menuId: string(),
    menuName: string(),
    items: json<
      readonly {
        readonly id: string;
        readonly label: string;
        readonly url: string;
        readonly children?: readonly {
          readonly id: string;
          readonly label: string;
          readonly url: string;
        }[];
      }[]
    >(),
    locations: json<readonly string[]>(),
    visibilityRules: json<{
      readonly roles?: readonly string[];
      readonly pages?: readonly string[];
    }>().optional(),
    cacheStrategy: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'menuId');

const redirectRule = table('redirectRule')
  .columns({
    workspaceId: string(),
    ruleId: string(),
    sourcePath: string(),
    targetPath: string(),
    statusCode: number(),
    matchType: enumeration<'exact' | 'prefix' | 'regex'>(),
    hitCount: number(),
    isActive: boolean(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'ruleId');

const cmsComment = table('cmsComment')
  .columns({
    workspaceId: string(),
    commentId: string(),
    articleId: string(),
    commenterName: string(),
    commenterEmail: string(),
    contentHtml: string(),
    contentPlain: string(),
    moderationStatus: enumeration<
      'pending' | 'approved' | 'rejected' | 'spam'
    >(),
    spamScore: number().optional(),
    upvotes: number(),
    downvotes: number(),
    parentCommentId: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'commentId');

const contentLock = table('contentLock')
  .columns({
    workspaceId: string(),
    lockId: string(),
    entityType: string(),
    entityId: string(),
    lockedByUserId: string(),
    lockedAt: string(),
    lockExpiry: string(),
    editSessionData: json<{
      readonly cursor?: {readonly line: number; readonly col: number};
    }>().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'lockId');

// CMS Domain Relationships

const project = table('project')
  .columns({
    workspaceId: string(),
    projectId: string(),
    projectName: string(),
    description: string().optional(),
    status: enumeration<
      'planning' | 'active' | 'on_hold' | 'completed' | 'cancelled'
    >(),
    startDate: number().optional(),
    endDate: number().optional(),
    budget: number().optional(),
    ownerId: string(),
    teamMemberIds: json<readonly string[]>(),
    settings: json<{
      readonly visibility: string;
      readonly notifications: boolean;
      readonly integrations: readonly string[];
    }>(),
    tags: json<readonly string[]>(),
    currency: string(),
    actualSpend: number(),
    progress: number(),
    priority: enumeration<'low' | 'medium' | 'high' | 'critical'>(),
    clientId: string().optional(),
    methodology: enumeration<'agile' | 'waterfall' | 'hybrid' | 'kanban'>(),
    customFields: json<{
      readonly [key: string]: string | number | boolean;
    }>().optional(),
    archivedAt: string().optional(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'projectId');

const task = table('task')
  .columns({
    workspaceId: string(),
    taskId: string(),
    projectId: string(),
    title: string(),
    descriptionMarkdown: string().optional(),
    descriptionHtml: string().optional(),
    assigneeIds: json<readonly string[]>(),
    dueDate: string().optional(),
    priority: enumeration<'low' | 'medium' | 'high' | 'critical'>(),
    status: enumeration<
      'todo' | 'in_progress' | 'review' | 'done' | 'blocked'
    >(),
    completionPercentage: number(),
    estimatedHours: number().optional(),
    actualHours: number().optional(),
    tags: json<readonly string[]>(),
    parentTaskId: string().optional(),
    subtaskIds: json<readonly string[]>(),
    watcherIds: json<readonly string[]>(),
    startedAt: string().optional(),
    completedAt: string().optional(),
    blockedReason: string().optional(),
    attachmentUrls: json<readonly string[]>(),
    customFieldValues: json<{
      readonly [key: string]: string | number | boolean;
    }>().optional(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'taskId');

const taskDependency = table('taskDependency')
  .columns({
    workspaceId: string(),
    dependencyId: string(),
    predecessorTaskId: string(),
    successorTaskId: string(),
    dependencyType: enumeration<
      | 'finish_to_start'
      | 'start_to_start'
      | 'finish_to_finish'
      | 'start_to_finish'
    >(),
    lagTimeHours: number(),
    constraintType: enumeration<'hard' | 'soft'>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'dependencyId');

const sprint = table('sprint')
  .columns({
    workspaceId: string(),
    sprintId: string(),
    projectId: string(),
    sprintName: string(),
    startDate: number(),
    endDate: number(),
    goal: string().optional(),
    velocity: number().optional(),
    capacity: number().optional(),
    status: enumeration<'planned' | 'active' | 'completed'>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'sprintId');

const board = table('board')
  .columns({
    workspaceId: string(),
    boardId: string(),
    boardName: string(),
    projectId: string(),
    columnsDefinition: json<
      readonly {
        readonly id: string;
        readonly name: string;
        readonly order: number;
        readonly wipLimit?: number;
      }[]
    >(),
    swimlaneConfig: json<{
      readonly enabled: boolean;
      readonly groupBy?: string;
    }>().optional(),
    cardTemplate: json<{readonly fields: readonly string[]}>(),
    filters: json<{readonly [key: string]: string}>().optional(),
    boardType: enumeration<'kanban' | 'scrum' | 'custom'>(),
    isTemplate: boolean(),
    templateDescription: string().optional(),
    automationRules: json<
      readonly {
        readonly trigger: string;
        readonly action: string;
        readonly conditions: readonly string[];
      }[]
    >(),
    memberIds: json<readonly string[]>(),
    visibility: enumeration<'private' | 'team' | 'organization'>(),
    backgroundImage: string().optional(),
    archivedAt: string().optional(),
    lastActivityAt: string(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'boardId');

const timeEntry = table('timeEntry')
  .columns({
    workspaceId: string(),
    entryId: string(),
    taskId: string(),
    userId: string(),
    hours: number(),
    billable: boolean(),
    description: string().optional(),
    startTime: string(),
    endTime: string(),
    billingRate: number().optional(),
    invoiceId: string().optional(),
    projectId: string(),
    clientId: string().optional(),
    currency: string(),
    approved: boolean(),
    approvedById: string().optional(),
    approvedAt: string().optional(),
    tags: json<readonly string[]>(),
    source: enumeration<'manual' | 'timer' | 'import'>(),
    metadata: json<{
      readonly device?: string;
      readonly location?: string;
    }>().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'entryId');

const taskComment = table('taskComment')
  .columns({
    workspaceId: string(),
    commentId: string(),
    taskId: string(),
    authorId: string(),
    contentMarkdown: string(),
    attachmentIds: json<readonly string[]>(),
    editedAt: string().optional(),
    reactions: json<{
      readonly [emoji: string]: readonly string[];
    }>().optional(),
    mentions: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'commentId');

const milestone = table('milestone')
  .columns({
    workspaceId: string(),
    milestoneId: string(),
    projectId: string(),
    name: string(),
    description: string().optional(),
    dueDate: string(),
    completionCriteria:
      json<
        readonly {readonly criterion: string; readonly completed: boolean}[]
      >(),
    status: enumeration<'pending' | 'in_progress' | 'completed' | 'missed'>(),
    dependencies: json<readonly string[]>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'milestoneId');

const projectBudget = table('projectBudget')
  .columns({
    workspaceId: string(),
    budgetId: string(),
    projectId: string(),
    budgetAmount: number(),
    spentAmount: number(),
    committedAmount: number(),
    forecast: number().optional(),
    periodStart: string(),
    periodEnd: string(),
    currency: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'budgetId');

const resourceAllocation = table('resourceAllocation')
  .columns({
    workspaceId: string(),
    allocationId: string(),
    projectId: string(),
    userId: string(),
    role: string(),
    capacityPercentage: number(),
    startDate: number(),
    endDate: number().optional(),
    costRate: number().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'allocationId');

// Project Management Domain Relationships

const employee = table('employee')
  .columns({
    workspaceId: string(),
    employeeId: string(),
    firstName: string(),
    lastName: string(),
    email: string(),
    phone: string().optional(),
    dateOfBirth: string(),
    hireDate: string(),
    terminationDate: string().optional(),
    departmentId: string(),
    jobTitle: string(),
    managerId: string().optional(),
    salary: number(),
    currency: string(),
    employmentType: enumeration<
      'full_time' | 'part_time' | 'contract' | 'intern'
    >(),
    status: enumeration<'active' | 'on_leave' | 'terminated'>(),
    address: json<{
      readonly street: string;
      readonly city: string;
      readonly state: string;
      readonly zip: string;
      readonly country: string;
    }>(),
    emergencyContact: json<{
      readonly name: string;
      readonly relationship: string;
      readonly phone: string;
    }>(),
    nationality: string().optional(),
    taxId: string().optional(),
    bankAccountInfo: json<{
      readonly accountNumber: string;
      readonly routingNumber: string;
      readonly bankName: string;
    }>().optional(),
    benefits: json<readonly string[]>(),
    skillTags: json<readonly string[]>(),
    profilePictureUrl: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'employeeId');

const payrollRun = table('payrollRun')
  .columns({
    workspaceId: string(),
    runId: string(),
    payPeriodStart: string(),
    payPeriodEnd: string(),
    processDate: string(),
    totalGross: number(),
    totalNet: number(),
    totalTaxes: number(),
    totalDeductions: number(),
    paymentMethod: enumeration<'direct_deposit' | 'check' | 'cash'>(),
    status: enumeration<'draft' | 'processing' | 'completed' | 'failed'>(),
    approvedById: string().optional(),
    approvedAt: string().optional(),
    currency: string(),
    employeeCount: number(),
    payFrequency: enumeration<
      'weekly' | 'biweekly' | 'semimonthly' | 'monthly'
    >(),
    taxFilings: json<
      readonly {
        readonly form: string;
        readonly amount: number;
        readonly filed: boolean;
      }[]
    >(),
    bankTransactionIds: json<readonly string[]>(),
    notes: string().optional(),
    paymentDate: string(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'runId');

const payrollLine = table('payrollLine')
  .columns({
    workspaceId: string(),
    lineId: string(),
    runId: string(),
    employeeId: string(),
    grossPay: number(),
    deductions:
      json<readonly {readonly type: string; readonly amount: number}[]>(),
    taxes: json<readonly {readonly type: string; readonly amount: number}[]>(),
    netPay: number(),
    hoursWorked: number().optional(),
    overtimeHours: number().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'lineId');

const timeOffRequest = table('timeOffRequest')
  .columns({
    workspaceId: string(),
    requestId: string(),
    employeeId: string(),
    leaveType: enumeration<
      'vacation' | 'sick' | 'personal' | 'unpaid' | 'parental'
    >(),
    startDate: number(),
    endDate: number(),
    daysCount: number(),
    reason: string().optional(),
    approvalStatus: enumeration<'pending' | 'approved' | 'rejected'>(),
    approverId: string().optional(),
    approvedAt: string().optional(),
    rejectionReason: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'requestId');

const benefitsEnrollment = table('benefitsEnrollment')
  .columns({
    workspaceId: string(),
    enrollmentId: string(),
    employeeId: string(),
    benefitPlanId: string(),
    coverageLevel: enumeration<
      'employee' | 'employee_spouse' | 'employee_children' | 'family'
    >(),
    premium: number(),
    dependents: json<
      readonly {
        readonly name: string;
        readonly relationship: string;
        readonly dob: string;
      }[]
    >(),
    effectiveDate: string(),
    endDate: number().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'enrollmentId');

const performanceReview = table('performanceReview')
  .columns({
    workspaceId: string(),
    reviewId: string(),
    employeeId: string(),
    reviewerId: string(),
    reviewPeriodStart: string(),
    reviewPeriodEnd: string(),
    ratings: json<
      readonly {
        readonly category: string;
        readonly score: number;
        readonly comments: string;
      }[]
    >(),
    goals:
      json<readonly {readonly goal: string; readonly achieved: boolean}[]>(),
    comments: string().optional(),
    overallScore: number(),
    reviewDate: string(),
    reviewType: enumeration<'annual' | 'quarterly' | 'probation' | '360'>(),
    status: enumeration<
      'draft' | 'in_progress' | 'completed' | 'acknowledged'
    >(),
    employeeAcknowledgedAt: string().optional(),
    employeeFeedback: string().optional(),
    improvementPlan: string().optional(),
    strengths: json<readonly string[]>(),
    developmentAreas: json<readonly string[]>(),
    nextReviewDate: string().optional(),
    compensationRecommendation: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'reviewId');

const department = table('department')
  .columns({
    workspaceId: string(),
    departmentId: string(),
    departmentName: string(),
    parentDepartmentId: string().optional(),
    headOfDepartmentId: string().optional(),
    budget: number().optional(),
    headcount: number(),
    costCenter: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'departmentId');

const compensationChange = table('compensationChange')
  .columns({
    workspaceId: string(),
    changeId: string(),
    employeeId: string(),
    changeType: enumeration<'raise' | 'bonus' | 'promotion' | 'adjustment'>(),
    oldAmount: number(),
    newAmount: number(),
    effectiveDate: string(),
    reason: string().optional(),
    approverId: string(),
    approvedAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'changeId');

const trainingRecord = table('trainingRecord')
  .columns({
    workspaceId: string(),
    recordId: string(),
    employeeId: string(),
    courseName: string(),
    completionDate: string(),
    score: number().optional(),
    certificateUrl: string().optional(),
    credits: number().optional(),
    provider: string(),
    expiryDate: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'recordId');

const attendanceLog = table('attendanceLog')
  .columns({
    workspaceId: string(),
    logId: string(),
    employeeId: string(),
    logDate: string(),
    clockInTime: string().optional(),
    clockOutTime: string().optional(),
    totalHours: number().optional(),
    location: string().optional(),
    approvalStatus: enumeration<'pending' | 'approved' | 'flagged'>(),
    anomalies:
      json<
        readonly {readonly type: string; readonly description: string}[]
      >().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'logId');

const ledgerAccount = table('ledgerAccount')
  .columns({
    workspaceId: string(),
    accountId: string(),
    accountNumber: string(),
    accountName: string(),
    accountType: enumeration<
      'asset' | 'liability' | 'equity' | 'revenue' | 'expense'
    >(),
    parentAccountId: string().optional(),
    currency: string(),
    balance: number(),
    status: enumeration<'active' | 'inactive'>(),
    taxRelevant: boolean(),
    description: string().optional(),
    normalBalance: enumeration<'debit' | 'credit'>(),
    isControlAccount: boolean(),
    allowJournalEntries: boolean(),
    cashFlowCategory: enumeration<
      'operating' | 'investing' | 'financing'
    >().optional(),
    financialStatement: enumeration<
      'balance_sheet' | 'income_statement' | 'cash_flow'
    >(),
    accountCategory: string(),
    externalId: string().optional(),
    lastActivityDate: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'accountId');

const journalEntry = table('journalEntry')
  .columns({
    workspaceId: string(),
    entryId: string(),
    entryNumber: string(),
    entryDate: string(),
    description: string(),
    reference: string().optional(),
    postedStatus: enumeration<'draft' | 'posted' | 'void'>(),
    reversalEntryId: string().optional(),
    createdById: string(),
    postedById: string().optional(),
    postedAt: string().optional(),
    periodId: string(),
    sourceType: enumeration<'manual' | 'system' | 'recurring' | 'closing'>(),
    isRecurring: boolean(),
    recurringSchedule: json<{
      readonly frequency: string;
      readonly nextDate: string;
    }>().optional(),
    attachmentUrls: json<readonly string[]>(),
    approvalStatus: enumeration<'pending' | 'approved' | 'rejected'>(),
    approvedById: string().optional(),
    approvedAt: string().optional(),
    tags: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'entryId');

const journalLine = table('journalLine')
  .columns({
    workspaceId: string(),
    lineId: string(),
    entryId: string(),
    accountId: string(),
    debitAmount: number(),
    creditAmount: number(),
    description: string().optional(),
    dimensions: json<{
      readonly department?: string;
      readonly project?: string;
      readonly location?: string;
    }>().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'lineId');

const invoice = table('invoice')
  .columns({
    workspaceId: string(),
    invoiceId: string(),
    invoiceNumber: string(),
    customerId: string(),
    issueDate: string(),
    dueDate: string(),
    lineItems: json<
      readonly {
        readonly description: string;
        readonly quantity: number;
        readonly unitPrice: number;
        readonly amount: number;
      }[]
    >(),
    subtotal: number(),
    taxAmount: number(),
    total: number(),
    currency: string(),
    paymentTerms: string(),
    status: enumeration<'draft' | 'sent' | 'paid' | 'overdue' | 'void'>(),
    notes: string().optional(),
    projectId: string().optional(),
    customerName: string(),
    customerEmail: string(),
    billingAddress: json<{
      readonly street: string;
      readonly city: string;
      readonly state: string;
      readonly zip: string;
      readonly country: string;
    }>(),
    paidAt: string().optional(),
    paidAmount: number(),
    discounts:
      json<readonly {readonly code: string; readonly amount: number}[]>(),
    attachmentUrls: json<readonly string[]>(),
    sentAt: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'invoiceId');

const payment = table('payment')
  .columns({
    workspaceId: string(),
    paymentId: string(),
    paymentDate: string(),
    payerName: string(),
    payeeName: string(),
    amount: number(),
    currency: string(),
    paymentMethod: enumeration<
      'cash' | 'check' | 'wire' | 'credit_card' | 'ach'
    >(),
    referenceNumber: string().optional(),
    appliedInvoices:
      json<readonly {readonly invoiceId: string; readonly amount: number}[]>(),
    createdById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'paymentId');

const bankTransaction = table('bankTransaction')
  .columns({
    workspaceId: string(),
    transactionId: string(),
    accountId: string(),
    transactionDate: string(),
    description: string(),
    amount: number(),
    balance: number(),
    category: string().optional(),
    reconciliationStatus: enumeration<'unreconciled' | 'reconciled'>(),
    reconciledAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'transactionId');

const expenseClaim = table('expenseClaim')
  .columns({
    workspaceId: string(),
    claimId: string(),
    employeeId: string(),
    claimDate: string(),
    items: json<
      readonly {
        readonly description: string;
        readonly category: string;
        readonly amount: number;
        readonly receiptUrl: string;
      }[]
    >(),
    totalAmount: number(),
    currency: string(),
    approvalChain: json<
      readonly {
        readonly userId: string;
        readonly status: string;
        readonly at?: string;
      }[]
    >(),
    reimbursedDate: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'claimId');

const budget = table('budget')
  .columns({
    workspaceId: string(),
    budgetId: string(),
    fiscalYear: number(),
    departmentId: string().optional(),
    accountId: string().optional(),
    budgetedAmount: number(),
    spentAmount: number(),
    committedAmount: number(),
    variance: number(),
    period: enumeration<'monthly' | 'quarterly' | 'yearly'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'budgetId');

const taxRate = table('taxRate')
  .columns({
    workspaceId: string(),
    rateId: string(),
    taxName: string(),
    ratePercentage: number(),
    jurisdiction: string(),
    effectiveFrom: string(),
    effectiveTo: string().optional(),
    compoundFlag: boolean(),
    accountMapping: string().optional(),
    status: enumeration<'active' | 'inactive'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'rateId');

const reconciliation = table('reconciliation')
  .columns({
    workspaceId: string(),
    reconciliationId: string(),
    accountId: string(),
    statementDate: string(),
    statementBalance: number(),
    bookBalance: number(),
    differences:
      json<
        readonly {readonly description: string; readonly amount: number}[]
      >(),
    status: enumeration<'in_progress' | 'completed' | 'discrepancy'>(),
    reconciledById: string(),
    reconciledAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'reconciliationId');

const patient = table('patient')
  .columns({
    workspaceId: string(),
    patientId: string(),
    mrn: string(),
    firstName: string(),
    lastName: string(),
    dateOfBirth: string(),
    gender: enumeration<'male' | 'female' | 'other' | 'unknown'>(),
    contactInfo: json<{
      readonly phone: string;
      readonly email?: string;
      readonly address: {
        readonly street: string;
        readonly city: string;
        readonly state: string;
        readonly zip: string;
      };
    }>(),
    insuranceInfo: json<
      readonly {
        readonly provider: string;
        readonly policyNumber: string;
        readonly groupNumber?: string;
        readonly effectiveDate: string;
      }[]
    >(),
    emergencyContacts: json<
      readonly {
        readonly name: string;
        readonly relationship: string;
        readonly phone: string;
      }[]
    >(),
    allergies: json<
      readonly {
        readonly allergen: string;
        readonly reaction: string;
        readonly severity: string;
      }[]
    >(),
    primaryPhysicianId: string().optional(),
    bloodType: string().optional(),
    ethnicity: string().optional(),
    preferredLanguage: string(),
    maritalStatus: enumeration<
      'single' | 'married' | 'divorced' | 'widowed'
    >().optional(),
    occupation: string().optional(),
    socialSecurityNumber: string().optional(),
    medications: json<
      readonly {
        readonly name: string;
        readonly dosage: string;
        readonly frequency: string;
      }[]
    >(),
    chronicConditions: json<readonly string[]>(),
    lastVisitDate: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'patientId');

const appointment = table('appointment')
  .columns({
    workspaceId: string(),
    appointmentId: string(),
    patientId: string(),
    providerId: string(),
    appointmentType: string(),
    startTime: string(),
    endTime: string(),
    location: string(),
    status: enumeration<
      | 'scheduled'
      | 'confirmed'
      | 'checked_in'
      | 'in_progress'
      | 'completed'
      | 'cancelled'
      | 'no_show'
    >(),
    reasonForVisit: string().optional(),
    notes: string().optional(),
    duration: number(),
    isVirtual: boolean(),
    virtualMeetingUrl: string().optional(),
    remindersSent:
      json<readonly {readonly type: string; readonly sentAt: string}[]>(),
    insuranceVerified: boolean(),
    copayAmount: number().optional(),
    cancellationReason: string().optional(),
    rescheduledFrom: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'appointmentId');

const medicalRecord = table('medicalRecord')
  .columns({
    workspaceId: string(),
    recordId: string(),
    patientId: string(),
    encounterId: string().optional(),
    encounterDate: string(),
    chiefComplaint: string(),
    vitals: json<{
      readonly temperature?: number;
      readonly bloodPressure?: string;
      readonly heartRate?: number;
      readonly respiratoryRate?: number;
      readonly weight?: number;
      readonly height?: number;
    }>(),
    diagnosisCodes: json<readonly string[]>(),
    treatmentPlan: string(),
    medications: json<
      readonly {
        readonly name: string;
        readonly dosage: string;
        readonly frequency: string;
      }[]
    >(),
    providerId: string(),
    encounterType: enumeration<
      'routine' | 'urgent' | 'followup' | 'emergency'
    >(),
    notes: string().optional(),
    labOrderIds: json<readonly string[]>(),
    imagingOrderIds: json<readonly string[]>(),
    referrals: json<
      readonly {
        readonly specialty: string;
        readonly providerId: string;
        readonly reason: string;
      }[]
    >(),
    billingCodes:
      json<readonly {readonly code: string; readonly description: string}[]>(),
    followUpDate: string().optional(),
    status: enumeration<'draft' | 'final' | 'amended'>(),
    signedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'recordId');

const prescription = table('prescription')
  .columns({
    workspaceId: string(),
    prescriptionId: string(),
    patientId: string(),
    medication: string(),
    dosage: string(),
    frequency: string(),
    quantity: number(),
    refills: number(),
    prescriberId: string(),
    pharmacyId: string().optional(),
    prescribedDate: string(),
    expiryDate: string().optional(),
    status: enumeration<'active' | 'filled' | 'cancelled' | 'expired'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'prescriptionId');

const labOrder = table('labOrder')
  .columns({
    workspaceId: string(),
    orderId: string(),
    patientId: string(),
    testPanel: string(),
    orderingProviderId: string(),
    priority: enumeration<'routine' | 'urgent' | 'stat'>(),
    collectionDate: string().optional(),
    labReference: string().optional(),
    status: enumeration<
      'ordered' | 'collected' | 'processing' | 'completed' | 'cancelled'
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'orderId');

const labResult = table('labResult')
  .columns({
    workspaceId: string(),
    resultId: string(),
    orderId: string(),
    testName: string(),
    resultValue: string(),
    referenceRange: string().optional(),
    units: string().optional(),
    abnormalFlag: boolean(),
    reviewedById: string().optional(),
    reviewedAt: string().optional(),
    resultDate: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'resultId');

const insuranceClaim = table('insuranceClaim')
  .columns({
    workspaceId: string(),
    claimId: string(),
    patientId: string(),
    encounterId: string(),
    claimNumber: string(),
    procedureCodes: json<
      readonly {
        readonly code: string;
        readonly description: string;
        readonly amount: number;
      }[]
    >(),
    diagnosisCodes: json<readonly string[]>(),
    billedAmount: number(),
    allowedAmount: number().optional(),
    paidAmount: number().optional(),
    payerId: string(),
    status: enumeration<
      'submitted' | 'pending' | 'approved' | 'denied' | 'appealed'
    >(),
    submittedAt: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'claimId');

const diagnosis = table('diagnosis')
  .columns({
    workspaceId: string(),
    diagnosisId: string(),
    patientId: string(),
    icdCode: string(),
    description: string(),
    diagnosisDate: string(),
    providerId: string(),
    status: enumeration<'active' | 'resolved' | 'chronic'>(),
    notes: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'diagnosisId');

const vitalSigns = table('vitalSigns')
  .columns({
    workspaceId: string(),
    vitalId: string(),
    patientId: string(),
    recordedAt: string(),
    bloodPressureSystolic: number().optional(),
    bloodPressureDiastolic: number().optional(),
    heartRate: number().optional(),
    temperature: number().optional(),
    weight: number().optional(),
    height: number().optional(),
    oxygenSaturation: number().optional(),
    recordedById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'vitalId');

const immunization = table('immunization')
  .columns({
    workspaceId: string(),
    immunizationId: string(),
    patientId: string(),
    vaccineName: string(),
    cvxCode: string().optional(),
    administeredDate: string(),
    lotNumber: string(),
    site: string(),
    route: string(),
    providerId: string(),
    nextDueDate: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'immunizationId');

const property = table('property')
  .columns({
    workspaceId: string(),
    propertyId: string(),
    address: json<{
      readonly street: string;
      readonly unit?: string;
      readonly city: string;
      readonly state: string;
      readonly zip: string;
      readonly country: string;
      readonly latitude?: number;
      readonly longitude?: number;
    }>(),
    propertyType: enumeration<
      | 'single_family'
      | 'condo'
      | 'townhouse'
      | 'multi_family'
      | 'land'
      | 'commercial'
    >(),
    bedrooms: number(),
    bathrooms: number(),
    sqft: number(),
    lotSizeSqft: number().optional(),
    yearBuilt: number(),
    features: json<readonly string[]>(),
    photos: json<
      readonly {
        readonly url: string;
        readonly caption?: string;
        readonly order: number;
      }[]
    >(),
    virtualTourUrl: string().optional(),
    propertyTaxes: number().optional(),
    hoaFees: number().optional(),
    utilities: json<{
      readonly electric?: number;
      readonly gas?: number;
      readonly water?: number;
    }>().optional(),
    parkingSpaces: number().optional(),
    garage: boolean(),
    basement: boolean(),
    pool: boolean(),
    heating: string().optional(),
    cooling: string().optional(),
    appliances: json<readonly string[]>(),
    flooring: json<readonly string[]>(),
    ownerId: string().optional(),
    agentId: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'propertyId');

const listing = table('listing')
  .columns({
    workspaceId: string(),
    listingId: string(),
    propertyId: string(),
    listPrice: number(),
    mlsNumber: string().optional(),
    listingAgentId: string(),
    coListingAgentId: string().optional(),
    listingDate: string(),
    expirationDate: string().optional(),
    status: enumeration<
      'coming_soon' | 'active' | 'pending' | 'sold' | 'withdrawn' | 'expired'
    >(),
    description: string(),
    publicRemarks: string(),
    privateRemarks: string().optional(),
    showingInstructions: string().optional(),
    lockboxCode: string().optional(),
    daysOnMarket: number(),
    viewCount: number(),
    inquiryCount: number(),
    showingCount: number(),
    offerCount: number(),
    priceHistory: json<
      readonly {
        readonly price: number;
        readonly date: string;
        readonly event: string;
      }[]
    >(),
    disclosures:
      json<readonly {readonly type: string; readonly document: string}[]>(),
    marketingPlan: json<{
      readonly channels: readonly string[];
      readonly budget?: number;
    }>().optional(),
    commissionRate: number(),
    commissionStructure: json<{
      readonly buyerAgent: number;
      readonly listingAgent: number;
    }>(),
    openHouseSchedule: json<
      readonly {
        readonly date: string;
        readonly startTime: string;
        readonly endTime: string;
      }[]
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'listingId');

const showing = table('showing')
  .columns({
    workspaceId: string(),
    showingId: string(),
    listingId: string(),
    scheduledTime: string(),
    duration: number(),
    buyerAgentId: string(),
    buyerAgentName: string(),
    buyerAgentPhone: string(),
    attendanceStatus: enumeration<
      'scheduled' | 'confirmed' | 'completed' | 'cancelled' | 'no_show'
    >(),
    feedback: string().optional(),
    rating: number().optional(),
    interestLevel: enumeration<'low' | 'medium' | 'high'>().optional(),
    notes: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'showingId');

const offer = table('offer')
  .columns({
    workspaceId: string(),
    offerId: string(),
    listingId: string(),
    buyerInfo: json<{
      readonly name: string;
      readonly email: string;
      readonly phone: string;
    }>(),
    buyerAgentId: string(),
    offerPrice: number(),
    earnestMoney: number(),
    downPayment: number(),
    financingType: enumeration<
      'cash' | 'conventional' | 'fha' | 'va' | 'other'
    >(),
    contingencies: json<
      readonly {
        readonly type: string;
        readonly description: string;
        readonly deadline?: string;
      }[]
    >(),
    expirationDate: string(),
    status: enumeration<
      | 'submitted'
      | 'reviewed'
      | 'countered'
      | 'accepted'
      | 'rejected'
      | 'withdrawn'
    >(),
    counterOffers: json<
      readonly {
        readonly price: number;
        readonly terms: string;
        readonly date: string;
      }[]
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'offerId');

const contract = table('contract')
  .columns({
    workspaceId: string(),
    contractId: string(),
    offerId: string(),
    listingId: string(),
    contractDate: string(),
    closingDate: string(),
    purchasePrice: number(),
    terms: json<{
      readonly financing: string;
      readonly earnestMoney: number;
      readonly downPayment: number;
      readonly closingCosts: {
        readonly buyer: number;
        readonly seller: number;
      };
    }>(),
    contingencies: json<
      readonly {
        readonly type: string;
        readonly description: string;
        readonly deadline: string;
        readonly waived: boolean;
      }[]
    >(),
    addendums: json<
      readonly {
        readonly title: string;
        readonly content: string;
        readonly addedAt: string;
      }[]
    >(),
    signatures: json<
      readonly {
        readonly party: string;
        readonly signedAt?: string;
        readonly signatureUrl?: string;
      }[]
    >(),
    status: enumeration<
      'pending' | 'contingent' | 'clear_to_close' | 'closed' | 'cancelled'
    >(),
    inspectionDate: string().optional(),
    inspectionResults: string().optional(),
    appraisalDate: string().optional(),
    appraisalValue: number().optional(),
    titleCompany: string().optional(),
    escrowNumber: string().optional(),
    buyerAgentId: string(),
    sellerAgentId: string(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'contractId');

const propertyDocument = table('propertyDocument')
  .columns({
    workspaceId: string(),
    documentId: string(),
    propertyId: string(),
    documentType: enumeration<
      | 'deed'
      | 'title'
      | 'survey'
      | 'inspection'
      | 'appraisal'
      | 'disclosure'
      | 'other'
    >(),
    fileName: string(),
    fileUrl: string(),
    uploadDate: string(),
    visibility: enumeration<'public' | 'agents' | 'parties' | 'private'>(),
    expirationDate: string().optional(),
    version: number(),
    uploadedById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'documentId');

const buyerProfile = table('buyerProfile')
  .columns({
    workspaceId: string(),
    buyerId: string(),
    name: string(),
    email: string(),
    phone: string(),
    budgetMin: number(),
    budgetMax: number(),
    desiredFeatures: json<readonly string[]>(),
    searchCriteria: json<{
      readonly locations: readonly string[];
      readonly propertyTypes: readonly string[];
      readonly bedrooms: {readonly min: number; readonly max?: number};
      readonly bathrooms: {readonly min: number; readonly max?: number};
    }>(),
    preApprovalAmount: number().optional(),
    preApprovalLetter: string().optional(),
    agentId: string().optional(),
    status: enumeration<'active' | 'inactive' | 'under_contract' | 'closed'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'buyerId');

const marketAnalysis = table('marketAnalysis')
  .columns({
    workspaceId: string(),
    analysisId: string(),
    propertyId: string(),
    comparableProperties: json<
      readonly {
        readonly propertyId: string;
        readonly address: string;
        readonly price: number;
        readonly similarity: number;
      }[]
    >(),
    estimatedValue: number(),
    valueLow: number(),
    valueHigh: number(),
    priceTrends: json<{
      readonly monthly: readonly number[];
      readonly yearly: readonly number[];
    }>(),
    neighborhoodData: json<{
      readonly medianPrice: number;
      readonly averageDaysOnMarket: number;
      readonly inventory: number;
    }>(),
    analyzedAt: string(),
    analyzedById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'analysisId');

const inspectionReport = table('inspectionReport')
  .columns({
    workspaceId: string(),
    reportId: string(),
    propertyId: string(),
    inspectionDate: string(),
    inspector: string(),
    inspectorLicense: string().optional(),
    findings: json<
      readonly {
        readonly category: string;
        readonly severity: string;
        readonly description: string;
        readonly repairEstimate?: number;
      }[]
    >(),
    summary: string(),
    reportUrl: string(),
    photosUrls: json<readonly string[]>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'reportId');

const commissionSplit = table('commissionSplit')
  .columns({
    workspaceId: string(),
    splitId: string(),
    contractId: string(),
    agentId: string(),
    commissionType: enumeration<'buyer_agent' | 'listing_agent' | 'referral'>(),
    percentage: number(),
    amount: number(),
    paymentStatus: enumeration<'pending' | 'paid' | 'disputed'>(),
    paidAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'splitId');

const course = table('course')
  .columns({
    workspaceId: string(),
    courseId: string(),
    courseName: string(),
    courseCode: string(),
    description: string(),
    syllabus: string().optional(),
    instructorIds: json<readonly string[]>(),
    prerequisites: json<readonly string[]>(),
    creditHours: number(),
    difficulty: enumeration<'beginner' | 'intermediate' | 'advanced'>(),
    category: string(),
    tags: json<readonly string[]>(),
    thumbnailUrl: string().optional(),
    estimatedHours: number(),
    language: string(),
    certificateOffered: boolean(),
    certificateTemplate: string().optional(),
    passingScore: number(),
    maxAttempts: number().optional(),
    validityPeriod: number().optional(),
    enrollmentCapacity: number().optional(),
    currentEnrollments: number(),
    startDate: number().optional(),
    endDate: number().optional(),
    status: enumeration<'draft' | 'published' | 'archived'>(),
    price: number().optional(),
    createdById: string(),
    updatedById: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'courseId');

const courseModule = table('courseModule')
  .columns({
    workspaceId: string(),
    moduleId: string(),
    courseId: string(),
    moduleName: string(),
    description: string().optional(),
    order: number(),
    estimatedHours: number(),
    unlockConditions: json<{
      readonly requiredModules?: readonly string[];
      readonly scoreThreshold?: number;
    }>().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'moduleId');

const lesson = table('lesson')
  .columns({
    workspaceId: string(),
    lessonId: string(),
    moduleId: string(),
    lessonTitle: string(),
    contentHtml: string(),
    contentMarkdown: string().optional(),
    videoUrl: string().optional(),
    attachments: json<
      readonly {
        readonly name: string;
        readonly url: string;
        readonly type: string;
      }[]
    >(),
    duration: number(),
    order: number(),
    completionCriteria: json<{
      readonly videoWatched?: boolean;
      readonly quizPassed?: boolean;
      readonly minTimeSpent?: number;
    }>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'lessonId');

const enrollment = table('enrollment')
  .columns({
    workspaceId: string(),
    enrollmentId: string(),
    courseId: string(),
    studentId: string(),
    enrolledDate: string(),
    progressPercentage: number(),
    completedModules: json<readonly string[]>(),
    currentModuleId: string().optional(),
    completionDate: string().optional(),
    finalGrade: number().optional(),
    certificateIssued: boolean(),
    certificateUrl: string().optional(),
    status: enumeration<
      'enrolled' | 'in_progress' | 'completed' | 'dropped' | 'failed'
    >(),
    lastAccessedAt: string().optional(),
    timeSpentMinutes: number(),
    notes: string().optional(),
    instructorFeedback: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'enrollmentId');

const assignment = table('assignment')
  .columns({
    workspaceId: string(),
    assignmentId: string(),
    courseId: string(),
    moduleId: string().optional(),
    title: string(),
    instructions: string(),
    dueDate: string(),
    maxPoints: number(),
    submissionType: enumeration<'text' | 'file' | 'url' | 'quiz'>(),
    rubric: json<
      readonly {
        readonly criterion: string;
        readonly points: number;
        readonly description: string;
      }[]
    >().optional(),
    allowLateSubmission: boolean(),
    latePenalty: number().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'assignmentId');

const submission = table('submission')
  .columns({
    workspaceId: string(),
    submissionId: string(),
    assignmentId: string(),
    studentId: string(),
    submittedAt: string(),
    contentText: string().optional(),
    fileUrls: json<readonly string[]>().optional(),
    grade: number().optional(),
    feedback: string().optional(),
    attemptNumber: number(),
    status: enumeration<'submitted' | 'graded' | 'returned' | 'resubmitted'>(),
    gradedAt: string().optional(),
    gradedById: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'submissionId');

const quiz = table('quiz')
  .columns({
    workspaceId: string(),
    quizId: string(),
    courseId: string(),
    moduleId: string().optional(),
    title: string(),
    description: string().optional(),
    questions: json<
      readonly {
        readonly id: string;
        readonly question: string;
        readonly type: string;
        readonly options?: readonly string[];
        readonly correctAnswer: string | readonly string[];
        readonly points: number;
      }[]
    >(),
    timeLimit: number().optional(),
    passingScore: number(),
    attemptsAllowed: number(),
    randomizeQuestions: boolean(),
    randomizeOptions: boolean(),
    showResults: enumeration<'immediate' | 'after_due' | 'manual'>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'quizId');

const quizAttempt = table('quizAttempt')
  .columns({
    workspaceId: string(),
    attemptId: string(),
    quizId: string(),
    studentId: string(),
    startedAt: string(),
    submittedAt: string().optional(),
    answers: json<{
      readonly [questionId: string]: string | readonly string[];
    }>(),
    score: number().optional(),
    passed: boolean().optional(),
    timeSpentSeconds: number(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'attemptId');

const discussionThread = table('discussionThread')
  .columns({
    workspaceId: string(),
    threadId: string(),
    courseId: string(),
    authorId: string(),
    title: string(),
    content: string(),
    repliesCount: number(),
    viewsCount: number(),
    isPinned: boolean(),
    isLocked: boolean(),
    tags: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'threadId');

const certificate = table('certificate')
  .columns({
    workspaceId: string(),
    certificateId: string(),
    enrollmentId: string(),
    studentId: string(),
    courseId: string(),
    issuedDate: string(),
    certificateNumber: string(),
    credentialUrl: string(),
    expirationDate: string().optional(),
    verificationCode: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'certificateId');

const video = table('video')
  .columns({
    workspaceId: string(),
    videoId: string(),
    title: string(),
    description: string(),
    channelId: string(),
    duration: number(),
    videoUrls: json<{
      readonly '360p'?: string;
      readonly '720p'?: string;
      readonly '1080p'?: string;
      readonly '4k'?: string;
      readonly 'hls'?: string;
      readonly 'dash'?: string;
    }>(),
    thumbnailUrl: string(),
    previewUrl: string().optional(),
    uploadDate: string(),
    publishedAt: string().optional(),
    viewCount: number(),
    likeCount: number(),
    dislikeCount: number(),
    commentCount: number(),
    shareCount: number(),
    contentRating: enumeration<'everyone' | 'teen' | 'mature' | 'restricted'>(),
    category: string(),
    tags: json<readonly string[]>(),
    language: string(),
    captionLanguages: json<readonly string[]>(),
    isLivestream: boolean(),
    visibility: enumeration<
      'public' | 'unlisted' | 'private' | 'members_only'
    >(),
    monetizationEnabled: boolean(),
    adsEnabled: boolean(),
    processingStatus: enumeration<
      'uploading' | 'processing' | 'ready' | 'failed'
    >(),
    uploadedById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'videoId');

const videoMetadata = table('videoMetadata')
  .columns({
    workspaceId: string(),
    videoId: string(),
    codecInfo: json<{
      readonly video: string;
      readonly audio: string;
      readonly container: string;
    }>(),
    bitrates: json<{
      readonly '360p'?: number;
      readonly '720p'?: number;
      readonly '1080p'?: number;
      readonly '4k'?: number;
    }>(),
    fileSizes: json<{
      readonly 'original': number;
      readonly '360p'?: number;
      readonly '720p'?: number;
      readonly '1080p'?: number;
      readonly '4k'?: number;
    }>(),
    resolution: json<{readonly width: number; readonly height: number}>(),
    frameRate: number(),
    aspectRatio: string(),
    processingStatus: enumeration<
      | 'queued'
      | 'transcoding'
      | 'generating_thumbnails'
      | 'completed'
      | 'failed'
    >(),
    processingProgress: number(),
    captionsSubtitles: json<
      readonly {
        readonly language: string;
        readonly url: string;
        readonly auto: boolean;
      }[]
    >(),
    chapterMarkers: json<
      readonly {
        readonly time: number;
        readonly title: string;
        readonly thumbnail?: string;
      }[]
    >(),
    thumbnails:
      json<readonly {readonly time: number; readonly url: string}[]>(),
    audioTracks: json<
      readonly {
        readonly language: string;
        readonly url: string;
        readonly default: boolean;
      }[]
    >(),
    sourceFileUrl: string(),
    sourceFileSize: number(),
    uploadCompletedAt: string(),
    transcodingCompletedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'videoId');

const playlist = table('playlist')
  .columns({
    workspaceId: string(),
    playlistId: string(),
    playlistName: string(),
    description: string().optional(),
    privacy: enumeration<'public' | 'unlisted' | 'private'>(),
    videoIds: json<readonly string[]>(),
    videoOrder:
      json<readonly {readonly videoId: string; readonly order: number}[]>(),
    thumbnailUrl: string().optional(),
    createdById: string(),
    subscriberCount: number(),
    viewCount: number(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'playlistId');

const subscription = table('subscription')
  .columns({
    workspaceId: string(),
    subscriptionId: string(),
    subscriberUserId: string(),
    channelId: string(),
    subscribedAt: string(),
    notificationPreferences: enumeration<'all' | 'personalized' | 'none'>(),
    tier: enumeration<'free' | 'basic' | 'premium'>().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'subscriptionId');

const watchHistory = table('watchHistory')
  .columns({
    workspaceId: string(),
    historyId: string(),
    userId: string(),
    videoId: string(),
    watchTimestamp: string(),
    progressSeconds: number(),
    completedFlag: boolean(),
    deviceType: string(),
    ipAddress: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'historyId');

const videoComment = table('videoComment')
  .columns({
    workspaceId: string(),
    commentId: string(),
    videoId: string(),
    userId: string(),
    commentText: string(),
    parentCommentId: string().optional(),
    timestampInVideo: number().optional(),
    likeCount: number(),
    isPinned: boolean(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'commentId');

const channel = table('channel')
  .columns({
    workspaceId: string(),
    channelId: string(),
    channelName: string(),
    handle: string(),
    description: string(),
    avatarUrl: string().optional(),
    bannerUrl: string().optional(),
    subscriberCount: number(),
    videoCount: number(),
    totalViews: number(),
    verificationStatus: enumeration<'unverified' | 'verified' | 'partner'>(),
    customUrl: string().optional(),
    joinedDate: string(),
    country: string().optional(),
    links: json<readonly {readonly title: string; readonly url: string}[]>(),
    monetizationEnabled: boolean(),
    ownerId: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'channelId');

const videoAnalytics = table('videoAnalytics')
  .columns({
    workspaceId: string(),
    analyticsId: string(),
    videoId: string(),
    date: string(),
    views: number(),
    watchTimeMinutes: number(),
    avgViewDuration: number(),
    trafficSources: json<{
      readonly search: number;
      readonly suggested: number;
      readonly external: number;
      readonly direct: number;
    }>(),
    demographics: json<{
      readonly ageGroups: {readonly [range: string]: number};
      readonly genderSplit: {
        readonly male: number;
        readonly female: number;
        readonly other: number;
      };
      readonly countries: {readonly [country: string]: number};
    }>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'analyticsId');

const livestream = table('livestream')
  .columns({
    workspaceId: string(),
    streamId: string(),
    channelId: string(),
    title: string(),
    description: string(),
    startedAt: string(),
    endedAt: string().optional(),
    currentViewers: number(),
    peakViewers: number(),
    chatEnabled: boolean(),
    streamKey: string(),
    streamUrl: string(),
    status: enumeration<'scheduled' | 'live' | 'ended'>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'streamId');

const contentRestriction = table('contentRestriction')
  .columns({
    workspaceId: string(),
    restrictionId: string(),
    videoId: string(),
    restrictionType: enumeration<
      'geographic' | 'age' | 'membership' | 'password'
    >(),
    regions: json<readonly string[]>().optional(),
    ageGate: number().optional(),
    membershipTier: string().optional(),
    password: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'restrictionId');

const profile = table('profile')
  .columns({
    workspaceId: string(),
    profileId: string(),
    userId: string(),
    username: string(),
    displayName: string(),
    bio: string().optional(),
    avatarUrl: string().optional(),
    coverPhotoUrl: string().optional(),
    website: string().optional(),
    location: string().optional(),
    followerCount: number(),
    followingCount: number(),
    postCount: number(),
    verifiedStatus: boolean(),
    accountType: enumeration<'personal' | 'business' | 'creator'>(),
    privacySettings: json<{
      readonly isPrivate: boolean;
      readonly showEmail: boolean;
      readonly showPhone: boolean;
      readonly allowMessages: string;
      readonly allowTagging: string;
    }>(),
    notificationSettings: json<{
      readonly likes: boolean;
      readonly comments: boolean;
      readonly follows: boolean;
      readonly mentions: boolean;
    }>(),
    socialLinks:
      json<readonly {readonly platform: string; readonly url: string}[]>(),
    interests: json<readonly string[]>(),
    badges:
      json<readonly {readonly type: string; readonly earnedAt: string}[]>(),
    joinedDate: string(),
    lastActiveAt: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'profileId');

const post = table('post')
  .columns({
    workspaceId: string(),
    postId: string(),
    authorId: string(),
    contentText: string().optional(),
    mediaAttachments: json<
      readonly {
        readonly type: string;
        readonly url: string;
        readonly thumbnail?: string;
        readonly width?: number;
        readonly height?: number;
        readonly duration?: number;
        readonly alt?: string;
      }[]
    >(),
    visibility: enumeration<'public' | 'followers' | 'friends' | 'private'>(),
    postType: enumeration<
      'text' | 'photo' | 'video' | 'link' | 'poll' | 'story'
    >(),
    location: json<{
      readonly name: string;
      readonly latitude?: number;
      readonly longitude?: number;
    }>().optional(),
    likeCount: number(),
    commentCount: number(),
    shareCount: number(),
    viewCount: number(),
    tags: json<readonly string[]>(),
    mentions: json<readonly string[]>(),
    hashtags: json<readonly string[]>(),
    isPinned: boolean(),
    isEdited: boolean(),
    editedAt: string().optional(),
    scheduledFor: string().optional(),
    expiresAt: string().optional(),
    repostOf: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'postId');

const postMedia = table('postMedia')
  .columns({
    workspaceId: string(),
    mediaId: string(),
    postId: string(),
    mediaType: enumeration<'image' | 'video' | 'gif' | 'audio'>(),
    url: string(),
    thumbnailUrl: string().optional(),
    width: number().optional(),
    height: number().optional(),
    duration: number().optional(),
    fileSize: number(),
    mimeType: string(),
    altText: string().optional(),
    order: number(),
    processingStatus: enumeration<
      'uploading' | 'processing' | 'ready' | 'failed'
    >(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'mediaId');

const followRelationship = table('followRelationship')
  .columns({
    workspaceId: string(),
    followerId: string(),
    followingId: string(),
    followedAt: string(),
    notificationEnabled: boolean(),
    relationshipStatus: enumeration<'following' | 'requested' | 'blocked'>(),
    mutualFollow: boolean(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'followerId', 'followingId');

const like = table('like')
  .columns({
    workspaceId: string(),
    likeId: string(),
    entityId: string(),
    entityType: enumeration<'post' | 'comment' | 'story'>(),
    userId: string(),
    likedAt: string(),
    reactionType: enumeration<
      'like' | 'love' | 'haha' | 'wow' | 'sad' | 'angry'
    >().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'likeId');

const comment = table('comment')
  .columns({
    workspaceId: string(),
    commentId: string(),
    postId: string(),
    authorId: string(),
    commentText: string(),
    parentCommentId: string().optional(),
    mentions: json<readonly string[]>(),
    likeCount: number(),
    replyCount: number(),
    isEdited: boolean(),
    editedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'commentId');

const directMessage = table('directMessage')
  .columns({
    workspaceId: string(),
    messageId: string(),
    conversationId: string(),
    senderId: string(),
    contentText: string().optional(),
    mediaAttachments: json<
      readonly {
        readonly type: string;
        readonly url: string;
        readonly thumbnail?: string;
      }[]
    >().optional(),
    replyToMessageId: string().optional(),
    reactions: json<{
      readonly [emoji: string]: readonly string[];
    }>().optional(),
    sentAt: string(),
    readAt: string().optional(),
    deletedForUsers: json<readonly string[]>(),
    isEdited: boolean(),
    editedAt: string().optional(),
    expiresAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'messageId');

const dmConversation = table('dmConversation')
  .columns({
    workspaceId: string(),
    conversationId: string(),
    participantIds: json<readonly string[]>(),
    lastMessageId: string().optional(),
    lastMessageAt: string().optional(),
    unreadCounts: json<{readonly [userId: string]: number}>(),
    mutedByUsers: json<readonly string[]>(),
    archivedByUsers: json<readonly string[]>(),
    conversationType: enumeration<'direct' | 'group'>(),
    groupName: string().optional(),
    groupAvatarUrl: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'conversationId');

const notification = table('notification')
  .columns({
    workspaceId: string(),
    notificationId: string(),
    userId: string(),
    notificationType: enumeration<
      'like' | 'comment' | 'follow' | 'mention' | 'share' | 'message'
    >(),
    actorId: string(),
    entityId: string(),
    entityType: string(),
    content: string(),
    readStatus: boolean(),
    readAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'notificationId');

const blockRelationship = table('blockRelationship')
  .columns({
    workspaceId: string(),
    blockId: string(),
    blockerUserId: string(),
    blockedUserId: string(),
    blockedAt: string(),
    reason: string().optional(),
    includesFollowers: boolean(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'blockId');

const legalCase = table('legalCase')
  .columns({
    workspaceId: string(),
    caseId: string(),
    caseNumber: string(),
    caseName: string(),
    caseType: enumeration<
      | 'civil'
      | 'criminal'
      | 'family'
      | 'corporate'
      | 'immigration'
      | 'bankruptcy'
    >(),
    court: string(),
    jurisdiction: string(),
    filingDate: string(),
    trialDate: string().optional(),
    leadAttorneyId: string(),
    assistingAttorneyIds: json<readonly string[]>(),
    status: enumeration<
      | 'open'
      | 'pending'
      | 'trial'
      | 'settled'
      | 'won'
      | 'lost'
      | 'appealed'
      | 'closed'
    >(),
    billingCode: string().optional(),
    estimatedValue: number().optional(),
    clientIds: json<readonly string[]>(),
    opposingCounsel: string().optional(),
    judge: string().optional(),
    description: string(),
    strategy: string().optional(),
    importantDates: json<
      readonly {
        readonly date: string;
        readonly description: string;
        readonly type: string;
      }[]
    >(),
    caseNotes: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'caseId');

const caseParty = table('caseParty')
  .columns({
    workspaceId: string(),
    partyId: string(),
    caseId: string(),
    partyType: enumeration<
      | 'plaintiff'
      | 'defendant'
      | 'petitioner'
      | 'respondent'
      | 'witness'
      | 'expert'
    >(),
    entityType: enumeration<'individual' | 'corporation' | 'government'>(),
    name: string(),
    contactInfo: json<{
      readonly email?: string;
      readonly phone?: string;
      readonly address?: string;
    }>(),
    representation: enumeration<'represented' | 'pro_se'>(),
    attorneyId: string().optional(),
    notes: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'partyId');

const legalDocument = table('legalDocument')
  .columns({
    workspaceId: string(),
    documentId: string(),
    caseId: string(),
    documentType: enumeration<
      | 'pleading'
      | 'motion'
      | 'brief'
      | 'order'
      | 'contract'
      | 'correspondence'
      | 'evidence'
      | 'other'
    >(),
    title: string(),
    description: string().optional(),
    fileUrl: string(),
    filedDate: string().optional(),
    filingDeadline: string().optional(),
    version: number(),
    status: enumeration<'draft' | 'filed' | 'served' | 'sealed'>(),
    accessControl: json<{
      readonly attorney: boolean;
      readonly client: boolean;
      readonly opposing: boolean;
    }>(),
    tags: json<readonly string[]>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'documentId');

const courtDate = table('courtDate')
  .columns({
    workspaceId: string(),
    dateId: string(),
    caseId: string(),
    hearingType: enumeration<
      | 'arraignment'
      | 'hearing'
      | 'trial'
      | 'deposition'
      | 'mediation'
      | 'conference'
    >(),
    courtLocation: string(),
    judge: string(),
    scheduledTime: string(),
    duration: number().optional(),
    attorneyIds: json<readonly string[]>(),
    preparationNotes: string().optional(),
    outcome: string().optional(),
    outcomeDate: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'dateId');

const billableTime = table('billableTime')
  .columns({
    workspaceId: string(),
    timeId: string(),
    caseId: string(),
    attorneyId: string(),
    activityType: enumeration<
      | 'research'
      | 'drafting'
      | 'court'
      | 'meeting'
      | 'phone'
      | 'email'
      | 'travel'
    >(),
    date: string(),
    hours: number(),
    rate: number(),
    description: string(),
    billedStatus: enumeration<'unbilled' | 'billed' | 'paid'>(),
    invoiceId: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'timeId');

const matter = table('matter')
  .columns({
    workspaceId: string(),
    matterId: string(),
    matterNumber: string(),
    clientId: string(),
    matterType: string(),
    description: string(),
    responsibleAttorneyId: string(),
    openDate: string(),
    closeDate: string().optional(),
    status: enumeration<'open' | 'pending' | 'closed'>(),
    practiceArea: string(),
    billingArrangement: enumeration<
      'hourly' | 'flat_fee' | 'contingency' | 'retainer'
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'matterId');

const trustTransaction = table('trustTransaction')
  .columns({
    workspaceId: string(),
    transactionId: string(),
    matterId: string(),
    transactionDate: string(),
    transactionType: enumeration<'deposit' | 'disbursement' | 'transfer'>(),
    amount: number(),
    balance: number(),
    reference: string().optional(),
    description: string(),
    createdById: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'transactionId');

const conflictCheck = table('conflictCheck')
  .columns({
    workspaceId: string(),
    checkId: string(),
    checkedEntityName: string(),
    relatedParties: json<readonly string[]>(),
    mattersFound: json<
      readonly {
        readonly matterId: string;
        readonly conflictType: string;
      }[]
    >(),
    clearedStatus: boolean(),
    checkedById: string(),
    notes: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'checkId');

const retainerAgreement = table('retainerAgreement')
  .columns({
    workspaceId: string(),
    agreementId: string(),
    matterId: string(),
    agreementDate: string(),
    feeStructure: enumeration<
      'hourly' | 'flat_fee' | 'contingency' | 'hybrid'
    >(),
    retainerAmount: number().optional(),
    hourlyRate: number().optional(),
    contingencyPercentage: number().optional(),
    billingTerms: string(),
    signedDocuments: json<readonly string[]>(),
    status: enumeration<'pending' | 'active' | 'completed' | 'terminated'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'agreementId');

const legalResearch = table('legalResearch')
  .columns({
    workspaceId: string(),
    researchId: string(),
    matterId: string(),
    issueDescription: string(),
    findings: string(),
    citations:
      json<
        readonly {readonly citation: string; readonly relevance: string}[]
      >(),
    researchHours: number(),
    attorneyId: string(),
    savedSearches:
      json<readonly {readonly query: string; readonly source: string}[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'researchId');

const menu = table('menu')
  .columns({
    workspaceId: string(),
    menuId: string(),
    menuName: string(),
    menuType: enumeration<
      | 'breakfast'
      | 'lunch'
      | 'dinner'
      | 'brunch'
      | 'dessert'
      | 'drinks'
      | 'specials'
    >(),
    active: boolean(),
    serviceHours: json<{
      readonly start: string;
      readonly end: string;
      readonly days: readonly string[];
    }>(),
    seasonal: boolean(),
    seasonStart: string().optional(),
    seasonEnd: string().optional(),
    menuSections: json<
      readonly {
        readonly name: string;
        readonly order: number;
        readonly description?: string;
      }[]
    >(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'menuId');

const menuItem = table('menuItem')
  .columns({
    workspaceId: string(),
    itemId: string(),
    menuId: string(),
    itemName: string(),
    description: string(),
    price: number(),
    cost: number().optional(),
    category: string(),
    section: string().optional(),
    dietaryFlags: json<readonly string[]>(),
    allergens: json<readonly string[]>(),
    calories: number().optional(),
    prepTime: number(),
    spiceLevel: number().optional(),
    availability: enumeration<'available' | 'out_of_stock' | '86ed'>(),
    popularityScore: number(),
    photoUrl: string().optional(),
    tags: json<readonly string[]>(),
    modifiers: json<
      readonly {
        readonly name: string;
        readonly price: number;
        readonly category: string;
      }[]
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'itemId');

const ingredient = table('ingredient')
  .columns({
    workspaceId: string(),
    ingredientId: string(),
    ingredientName: string(),
    unitOfMeasure: string(),
    currentStock: number(),
    reorderLevel: number(),
    reorderQuantity: number(),
    costPerUnit: number(),
    supplierId: string().optional(),
    allergens: json<readonly string[]>(),
    storageLocation: string().optional(),
    expiryTracking: boolean(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'ingredientId');

const recipe = table('recipe')
  .columns({
    workspaceId: string(),
    recipeId: string(),
    menuItemId: string(),
    ingredientQuantities: json<
      readonly {
        readonly ingredientId: string;
        readonly quantity: number;
        readonly unit: string;
      }[]
    >(),
    preparationSteps: json<
      readonly {
        readonly step: number;
        readonly instruction: string;
        readonly time: number;
      }[]
    >(),
    cookTime: number(),
    prepTime: number(),
    yieldPortions: number(),
    version: number(),
    notes: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'recipeId');

const restaurantOrder = table('restaurantOrder')
  .columns({
    workspaceId: string(),
    restaurantOrderId: string(),
    orderNumber: string(),
    orderType: enumeration<'dine_in' | 'takeout' | 'delivery' | 'curbside'>(),
    tableNumber: string().optional(),
    customerName: string().optional(),
    customerPhone: string().optional(),
    subtotal: number(),
    tax: number(),
    tip: number(),
    total: number(),
    status: enumeration<
      'pending' | 'preparing' | 'ready' | 'served' | 'completed' | 'cancelled'
    >(),
    serverId: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'restaurantOrderId');

const restaurantOrderItem = table('restaurantOrderItem')
  .columns({
    workspaceId: string(),
    restaurantOrderItemId: string(),
    restaurantOrderId: string(),
    menuItemId: string(),
    quantity: number(),
    modifications: json<
      readonly {
        readonly type: string;
        readonly value: string;
        readonly price: number;
      }[]
    >(),
    specialInstructions: string().optional(),
    courseTiming: enumeration<'appetizer' | 'main' | 'dessert'>().optional(),
    prepStatus: enumeration<'queued' | 'preparing' | 'ready' | 'served'>(),
    sentToKitchenAt: string().optional(),
    readyAt: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'restaurantOrderItemId');

const restaurantReservation = table('restaurantReservation')
  .columns({
    workspaceId: string(),
    restaurantReservationId: string(),
    reservationDate: string(),
    reservationTime: string(),
    partySize: number(),
    customerName: string(),
    customerPhone: string(),
    customerEmail: string().optional(),
    tableAssignment: string().optional(),
    specialRequests: string().optional(),
    status: enumeration<
      'pending' | 'confirmed' | 'seated' | 'completed' | 'cancelled' | 'no_show'
    >(),
    confirmedAt: string().optional(),
    seatedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'restaurantReservationId');

const supplier = table('supplier')
  .columns({
    workspaceId: string(),
    supplierId: string(),
    supplierName: string(),
    contactName: string().optional(),
    contactPhone: string(),
    contactEmail: string(),
    address: string().optional(),
    paymentTerms: string(),
    deliverySchedule:
      json<readonly {readonly day: string; readonly time: string}[]>(),
    productCategories: json<readonly string[]>(),
    rating: number().optional(),
    accountNumber: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'supplierId');

const purchaseOrder = table('purchaseOrder')
  .columns({
    workspaceId: string(),
    poId: string(),
    supplierId: string(),
    orderDate: string(),
    items: json<
      readonly {
        readonly ingredientId: string;
        readonly quantity: number;
        readonly unitPrice: number;
      }[]
    >(),
    totalAmount: number(),
    expectedDelivery: string(),
    receivedDate: string().optional(),
    invoiceMatching: boolean(),
    status: enumeration<
      'draft' | 'sent' | 'received' | 'partial' | 'cancelled'
    >(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'poId');

const inventoryCount = table('inventoryCount')
  .columns({
    workspaceId: string(),
    countId: string(),
    countDate: string(),
    ingredientId: string(),
    countedQuantity: number(),
    expectedQuantity: number(),
    variance: number(),
    countLocation: string(),
    countedById: string(),
    notes: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'countId');

const shipment = table('shipment')
  .columns({
    workspaceId: string(),
    shipmentId: string(),
    trackingNumber: string(),
    origin: json<{
      readonly address: string;
      readonly city: string;
      readonly state: string;
      readonly zip: string;
      readonly country: string;
    }>(),
    destination: json<{
      readonly address: string;
      readonly city: string;
      readonly state: string;
      readonly zip: string;
      readonly country: string;
    }>(),
    carrier: string(),
    serviceLevel: enumeration<
      'standard' | 'expedited' | 'overnight' | 'international'
    >(),
    weight: number(),
    dimensions: json<{
      readonly length: number;
      readonly width: number;
      readonly height: number;
      readonly unit: string;
    }>(),
    declaredValue: number(),
    status: enumeration<
      | 'created'
      | 'picked_up'
      | 'in_transit'
      | 'out_for_delivery'
      | 'delivered'
      | 'exception'
      | 'returned'
    >(),
    estimatedDelivery: string(),
    actualDelivery: string().optional(),
    shippingCost: number(),
    insuranceCost: number().optional(),
    signatureRequired: boolean(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'shipmentId');

const shipmentItem = table('shipmentItem')
  .columns({
    workspaceId: string(),
    itemId: string(),
    shipmentId: string(),
    sku: string(),
    description: string(),
    quantity: number(),
    weight: number(),
    value: number(),
    hsCode: string().optional(),
    countryOfOrigin: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'itemId');

const route = table('route')
  .columns({
    workspaceId: string(),
    routeId: string(),
    routeName: string(),
    waypoints: json<
      readonly {
        readonly address: string;
        readonly latitude: number;
        readonly longitude: number;
        readonly stopNumber: number;
      }[]
    >(),
    distance: number(),
    estimatedDuration: number(),
    driverId: string(),
    vehicleId: string(),
    status: enumeration<
      'planned' | 'in_progress' | 'completed' | 'cancelled'
    >(),
    startedAt: string().optional(),
    completedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'routeId');

const trackingEvent = table('trackingEvent')
  .columns({
    workspaceId: string(),
    eventId: string(),
    shipmentId: string(),
    eventType: enumeration<
      | 'pickup'
      | 'departure'
      | 'arrival'
      | 'in_transit'
      | 'out_for_delivery'
      | 'delivered'
      | 'exception'
    >(),
    location: string(),
    timestamp: string(),
    statusCode: string(),
    exceptionDetails: string().optional(),
    scanData: json<{
      readonly scannedBy?: string;
      readonly facility?: string;
    }>().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'eventId');

const driver = table('driver')
  .columns({
    workspaceId: string(),
    driverId: string(),
    driverName: string(),
    licenseNumber: string(),
    licenseExpiry: string(),
    certifications:
      json<readonly {readonly type: string; readonly expiry: string}[]>(),
    phone: string(),
    email: string().optional(),
    status: enumeration<'available' | 'on_route' | 'off_duty' | 'inactive'>(),
    currentLocation: json<{
      readonly latitude: number;
      readonly longitude: number;
    }>().optional(),
    vehicleAssigned: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'driverId');

const vehicle = table('vehicle')
  .columns({
    workspaceId: string(),
    vehicleId: string(),
    vehicleNumber: string(),
    vehicleType: enumeration<'van' | 'truck' | 'semi' | 'trailer'>(),
    make: string(),
    model: string(),
    year: number(),
    vin: string(),
    capacity: json<{readonly weight: number; readonly volume: number}>(),
    fuelType: string(),
    maintenanceDue: string().optional(),
    insuranceExpiry: string(),
    status: enumeration<'active' | 'maintenance' | 'retired'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'vehicleId');

const warehouse = table('warehouse')
  .columns({
    workspaceId: string(),
    warehouseId: string(),
    facilityName: string(),
    address: string(),
    capacity: number(),
    operatingHours: json<{
      readonly weekday: {readonly open: string; readonly close: string};
      readonly weekend?: {readonly open: string; readonly close: string};
    }>(),
    managerId: string().optional(),
    zones: json<
      readonly {
        readonly zoneId: string;
        readonly name: string;
        readonly type: string;
      }[]
    >(),
    inventoryCount: number(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'warehouseId');

const warehouseZone = table('warehouseZone')
  .columns({
    workspaceId: string(),
    zoneId: string(),
    warehouseId: string(),
    zoneName: string(),
    zoneType: enumeration<
      'receiving' | 'storage' | 'picking' | 'packing' | 'shipping'
    >(),
    capacity: number(),
    temperatureControlled: boolean(),
    currentUtilization: number(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'zoneId');

const deliveryManifest = table('deliveryManifest')
  .columns({
    workspaceId: string(),
    manifestId: string(),
    routeId: string(),
    shipmentIds: json<readonly string[]>(),
    sequenceOrder: json<
      readonly {
        readonly shipmentId: string;
        readonly stopNumber: number;
      }[]
    >(),
    estimatedTimes:
      json<readonly {readonly stopNumber: number; readonly time: string}[]>(),
    actualTimes:
      json<
        readonly {readonly stopNumber: number; readonly time: string}[]
      >().optional(),
    proofOfDelivery: json<
      readonly {
        readonly shipmentId: string;
        readonly signature: string;
        readonly photo?: string;
      }[]
    >().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'manifestId');

const freightInvoice = table('freightInvoice')
  .columns({
    workspaceId: string(),
    invoiceId: string(),
    shipmentId: string(),
    invoiceNumber: string(),
    baseRate: number(),
    surcharges:
      json<readonly {readonly type: string; readonly amount: number}[]>(),
    total: number(),
    currency: string(),
    paymentTerms: string(),
    status: enumeration<'draft' | 'sent' | 'paid' | 'overdue'>(),
    paidAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'invoiceId');

const policy = table('policy')
  .columns({
    workspaceId: string(),
    policyId: string(),
    policyNumber: string(),
    policyType: enumeration<
      'auto' | 'home' | 'life' | 'health' | 'business' | 'umbrella'
    >(),
    policyholderId: string(),
    coverageAmounts: json<{readonly [coverageType: string]: number}>(),
    deductibles: json<{readonly [coverageType: string]: number}>(),
    premium: number(),
    premiumFrequency: enumeration<'monthly' | 'quarterly' | 'annually'>(),
    effectiveDate: string(),
    expiryDate: string(),
    status: enumeration<'active' | 'lapsed' | 'cancelled' | 'expired'>(),
    agentId: string(),
    underwriterId: string().optional(),
    beneficiaries: json<
      readonly {
        readonly name: string;
        readonly relationship: string;
        readonly percentage: number;
      }[]
    >().optional(),
    riders: json<
      readonly {
        readonly type: string;
        readonly coverage: number;
        readonly premium: number;
      }[]
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'policyId');

const policyCoverage = table('policyCoverage')
  .columns({
    workspaceId: string(),
    coverageId: string(),
    policyId: string(),
    coverageType: string(),
    limit: number(),
    deductible: number(),
    premiumAllocation: number(),
    endorsements:
      json<readonly {readonly type: string; readonly description: string}[]>(),
    riders:
      json<readonly {readonly name: string; readonly coverage: number}[]>(),
    effectiveDate: string(),
    expiryDate: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'coverageId');

const claim = table('claim')
  .columns({
    workspaceId: string(),
    claimId: string(),
    policyId: string(),
    claimNumber: string(),
    incidentDate: string(),
    reportedDate: string(),
    claimType: string(),
    description: string(),
    amountRequested: number(),
    amountApproved: number().optional(),
    amountPaid: number().optional(),
    status: enumeration<
      'reported' | 'investigating' | 'approved' | 'denied' | 'paid' | 'closed'
    >(),
    adjusterId: string(),
    documents: json<readonly {readonly type: string; readonly url: string}[]>(),
    notes: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'claimId');

const claimPayment = table('claimPayment')
  .columns({
    workspaceId: string(),
    paymentId: string(),
    claimId: string(),
    paymentDate: string(),
    payee: string(),
    amount: number(),
    paymentMethod: enumeration<'check' | 'direct_deposit' | 'wire'>(),
    checkNumber: string().optional(),
    transactionReference: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'paymentId');

const underwritingReview = table('underwritingReview')
  .columns({
    workspaceId: string(),
    reviewId: string(),
    applicationId: string(),
    reviewerId: string(),
    riskScore: number(),
    approvalStatus: enumeration<'approved' | 'declined' | 'referred'>(),
    conditions: json<readonly string[]>().optional(),
    premiumAdjustment: number().optional(),
    notes: string(),
    reviewedAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'reviewId');

const policyDocument = table('policyDocument')
  .columns({
    workspaceId: string(),
    documentId: string(),
    policyId: string(),
    documentType: enumeration<
      'policy' | 'endorsement' | 'declaration' | 'certificate' | 'cancellation'
    >(),
    fileUrl: string(),
    version: number(),
    issuedDate: string(),
    language: string(),
    signatures:
      json<readonly {readonly party: string; readonly signedAt?: string}[]>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'documentId');

const premiumPayment = table('premiumPayment')
  .columns({
    workspaceId: string(),
    paymentId: string(),
    policyId: string(),
    dueDate: string(),
    paidDate: string().optional(),
    amount: number(),
    paymentMethod: string().optional(),
    lateFee: number().optional(),
    receiptNumber: string().optional(),
    status: enumeration<'pending' | 'paid' | 'overdue' | 'waived'>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'paymentId');

const adjusterAssignment = table('adjusterAssignment')
  .columns({
    workspaceId: string(),
    assignmentId: string(),
    claimId: string(),
    adjusterId: string(),
    assignedDate: string(),
    workloadScore: number(),
    specializationMatch: number(),
    status: enumeration<'assigned' | 'investigating' | 'completed'>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'assignmentId');

const lossHistory = table('lossHistory')
  .columns({
    workspaceId: string(),
    lossId: string(),
    policyholderId: string(),
    lossDate: string(),
    lossType: string(),
    amountPaid: number(),
    claimId: string().optional(),
    faultDetermination: enumeration<
      'at_fault' | 'not_at_fault' | 'partial'
    >().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'lossId');

const reinsuranceTreaty = table('reinsuranceTreaty')
  .columns({
    workspaceId: string(),
    treatyId: string(),
    treatyName: string(),
    reinsurer: string(),
    coverageType: string(),
    retention: number(),
    limit: number(),
    commissionPercentage: number(),
    effectiveDate: string(),
    expiryDate: string(),
    status: enumeration<'active' | 'expired' | 'cancelled'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'treatyId');

const room = table('room')
  .columns({
    workspaceId: string(),
    roomId: string(),
    roomNumber: string(),
    roomType: string(),
    floor: number(),
    beds: json<readonly {readonly type: string; readonly count: number}[]>(),
    maxOccupancy: number(),
    amenities: json<readonly string[]>(),
    accessibilityFeatures: json<readonly string[]>(),
    viewType: string().optional(),
    squareFeet: number().optional(),
    maintenanceStatus: enumeration<
      'available' | 'occupied' | 'maintenance' | 'cleaning'
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'roomId');

const reservation = table('reservation')
  .columns({
    workspaceId: string(),
    reservationId: string(),
    confirmationNumber: string(),
    guestId: string(),
    roomAssignments: json<readonly string[]>(),
    checkInDate: string(),
    checkOutDate: string(),
    numberOfGuests: number(),
    rateCode: string(),
    roomRate: number(),
    totalCharges: number(),
    status: enumeration<
      | 'pending'
      | 'confirmed'
      | 'checked_in'
      | 'checked_out'
      | 'cancelled'
      | 'no_show'
    >(),
    specialRequests: string().optional(),
    bookedVia: enumeration<
      'direct' | 'phone' | 'online' | 'travel_agent' | 'ota'
    >(),
    depositPaid: number().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'reservationId');

const guest = table('guest')
  .columns({
    workspaceId: string(),
    guestId: string(),
    firstName: string(),
    lastName: string(),
    email: string(),
    phone: string(),
    address: json<{
      readonly street?: string;
      readonly city?: string;
      readonly state?: string;
      readonly zip?: string;
      readonly country?: string;
    }>().optional(),
    loyaltyNumber: string().optional(),
    loyaltyTier: enumeration<
      'standard' | 'silver' | 'gold' | 'platinum'
    >().optional(),
    preferences: json<{
      readonly roomType?: string;
      readonly floor?: string;
      readonly bedding?: string;
      readonly pillows?: string;
    }>(),
    vipStatus: boolean(),
    documentVerification: json<{
      readonly type: string;
      readonly number: string;
      readonly verified: boolean;
    }>().optional(),
    stayHistoryCount: number(),
    totalSpent: number(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'guestId');

const ratePlan = table('ratePlan')
  .columns({
    workspaceId: string(),
    planId: string(),
    planName: string(),
    roomType: string(),
    baseRate: number(),
    seasonalAdjustments: json<
      readonly {
        readonly startDate: string;
        readonly endDate: string;
        readonly rateModifier: number;
      }[]
    >(),
    restrictions: json<{
      readonly minStay?: number;
      readonly maxStay?: number;
      readonly advanceBooking?: number;
    }>().optional(),
    refundPolicy: enumeration<'refundable' | 'non_refundable' | 'partial'>(),
    cancellationDeadline: number(),
    blackoutDates: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'planId');

const housekeepingTask = table('housekeepingTask')
  .columns({
    workspaceId: string(),
    taskId: string(),
    roomId: string(),
    taskType: enumeration<
      'checkout_clean' | 'stayover_service' | 'deep_clean' | 'turndown'
    >(),
    priority: enumeration<'low' | 'medium' | 'high' | 'urgent'>(),
    assignedTo: string().optional(),
    scheduledTime: string(),
    completedTime: string().optional(),
    inspectionNotes: string().optional(),
    status: enumeration<
      'pending' | 'in_progress' | 'completed' | 'inspection_required'
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'taskId');

const guestServiceRequest = table('guestServiceRequest')
  .columns({
    workspaceId: string(),
    requestId: string(),
    reservationId: string(),
    guestId: string(),
    requestType: enumeration<
      | 'housekeeping'
      | 'maintenance'
      | 'concierge'
      | 'room_service'
      | 'amenity'
      | 'other'
    >(),
    description: string(),
    priority: enumeration<'low' | 'medium' | 'high'>(),
    assignedTo: string().optional(),
    status: enumeration<
      'received' | 'in_progress' | 'completed' | 'cancelled'
    >(),
    resolutionNotes: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'requestId');

const amenityBooking = table('amenityBooking')
  .columns({
    workspaceId: string(),
    bookingId: string(),
    reservationId: string(),
    guestId: string(),
    amenityType: enumeration<
      'spa' | 'restaurant' | 'golf' | 'fitness' | 'pool' | 'event_space'
    >(),
    bookingTime: string(),
    duration: number(),
    numberOfGuests: number(),
    cost: number(),
    status: enumeration<'confirmed' | 'completed' | 'cancelled'>(),
    specialRequests: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'bookingId');

const roomRateOverride = table('roomRateOverride')
  .columns({
    workspaceId: string(),
    overrideId: string(),
    reservationId: string(),
    originalRate: number(),
    overrideRate: number(),
    reason: string(),
    approvedById: string(),
    dateRange: json<{readonly start: string; readonly end: string}>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'overrideId');

const folioCharge = table('folioCharge')
  .columns({
    workspaceId: string(),
    chargeId: string(),
    reservationId: string(),
    chargeDate: string(),
    chargeType: enumeration<
      'room' | 'food' | 'beverage' | 'amenity' | 'tax' | 'other'
    >(),
    description: string(),
    amount: number(),
    tax: number(),
    postedById: string(),
    paymentMethod: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'chargeId');

const maintenanceIssue = table('maintenanceIssue')
  .columns({
    workspaceId: string(),
    issueId: string(),
    roomId: string().optional(),
    areaReference: string().optional(),
    issueType: enumeration<
      'plumbing' | 'electrical' | 'hvac' | 'appliance' | 'structural' | 'other'
    >(),
    description: string(),
    priority: enumeration<'low' | 'medium' | 'high' | 'emergency'>(),
    reportedById: string(),
    assignedTo: string().optional(),
    resolutionStatus: enumeration<
      'reported' | 'assigned' | 'in_progress' | 'resolved' | 'deferred'
    >(),
    resolvedAt: string().optional(),
    cost: number().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'issueId');

const marketingCampaign = table('marketingCampaign')
  .columns({
    workspaceId: string(),
    campaignId: string(),
    campaignName: string(),
    campaignType: enumeration<
      'email' | 'social' | 'display' | 'search' | 'content' | 'multi_channel'
    >(),
    startDate: number(),
    endDate: number().optional(),
    budget: number(),
    channels: json<readonly string[]>(),
    goals:
      json<readonly {readonly metric: string; readonly target: number}[]>(),
    attributionModel: enumeration<
      'first_touch' | 'last_touch' | 'multi_touch' | 'time_decay'
    >(),
    status: enumeration<
      'draft' | 'scheduled' | 'active' | 'paused' | 'completed'
    >(),
    targetAudience: json<{
      readonly segments: readonly string[];
      readonly criteria: {readonly [key: string]: string};
    }>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'campaignId');

const lead = table('lead')
  .columns({
    workspaceId: string(),
    leadId: string(),
    email: string(),
    firstName: string().optional(),
    lastName: string().optional(),
    phone: string().optional(),
    company: string().optional(),
    jobTitle: string().optional(),
    leadSource: string(),
    score: number(),
    stage: enumeration<
      'new' | 'contacted' | 'qualified' | 'converted' | 'lost'
    >(),
    qualificationStatus: enumeration<
      'unqualified' | 'marketing_qualified' | 'sales_qualified'
    >(),
    assignedTo: string().optional(),
    customFields: json<{
      readonly [key: string]: string | number | boolean;
    }>(),
    tags: json<readonly string[]>(),
    lastActivityAt: string().optional(),
    convertedAt: string().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'leadId');

const segment = table('segment')
  .columns({
    workspaceId: string(),
    segmentId: string(),
    segmentName: string(),
    description: string().optional(),
    filterCriteria: json<{
      readonly conditions: readonly {
        readonly field: string;
        readonly operator: string;
        readonly value: string;
      }[];
      readonly logic: string;
    }>(),
    memberCount: number(),
    segmentType: enumeration<'static' | 'dynamic'>(),
    refreshFrequency: enumeration<
      'realtime' | 'hourly' | 'daily' | 'manual'
    >().optional(),
    lastRefreshedAt: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'segmentId');

const customerJourney = table('customerJourney')
  .columns({
    workspaceId: string(),
    journeyId: string(),
    journeyName: string(),
    description: string().optional(),
    triggerConditions: json<{
      readonly event: string;
      readonly conditions: readonly {
        readonly field: string;
        readonly value: string;
      }[];
    }>(),
    steps: json<
      readonly {
        readonly stepId: string;
        readonly type: string;
        readonly config: {
          readonly [key: string]: string | number | boolean;
        };
        readonly delay?: number;
      }[]
    >(),
    active: boolean(),
    conversionGoal: string(),
    analytics: json<{
      readonly enrolled: number;
      readonly completed: number;
      readonly converted: number;
      readonly dropoff: {readonly [stepId: string]: number};
    }>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'journeyId');

const abTest = table('abTest')
  .columns({
    workspaceId: string(),
    testId: string(),
    testName: string(),
    description: string().optional(),
    variants: json<
      readonly {
        readonly variantId: string;
        readonly name: string;
        readonly content: {readonly [key: string]: string};
        readonly weight: number;
      }[]
    >(),
    trafficSplit: json<{readonly [variantId: string]: number}>(),
    startDate: number(),
    endDate: number().optional(),
    metricTracked: string(),
    winningVariant: string().optional(),
    statisticalSignificance: number().optional(),
    status: enumeration<'draft' | 'running' | 'paused' | 'completed'>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'testId');

const leadActivity = table('leadActivity')
  .columns({
    workspaceId: string(),
    activityId: string(),
    leadId: string(),
    activityType: enumeration<
      | 'email_open'
      | 'email_click'
      | 'page_view'
      | 'form_submit'
      | 'download'
      | 'event_attendance'
    >(),
    timestamp: string(),
    channel: string(),
    contentReference: string().optional(),
    scoreDelta: number(),
    attributionData: json<{
      readonly campaign?: string;
      readonly source?: string;
      readonly medium?: string;
    }>(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'activityId');

const attributionTouchpoint = table('attributionTouchpoint')
  .columns({
    workspaceId: string(),
    touchpointId: string(),
    leadId: string(),
    contactId: string().optional(),
    channel: string(),
    campaignId: string().optional(),
    timestamp: string(),
    positionInJourney: number(),
    creditPercentage: number(),
    conversionValue: number().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'touchpointId');

const conversionGoal = table('conversionGoal')
  .columns({
    workspaceId: string(),
    goalId: string(),
    goalName: string(),
    goalType: enumeration<
      'lead' | 'sale' | 'signup' | 'download' | 'engagement' | 'custom'
    >(),
    targetValue: number(),
    conversionCriteria: json<{
      readonly event: string;
      readonly conditions: readonly {
        readonly field: string;
        readonly value: string;
      }[];
    }>(),
    timeWindow: number(),
    funnelSteps:
      json<readonly {readonly step: string; readonly order: number}[]>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'goalId');

const marketingAsset = table('marketingAsset')
  .columns({
    workspaceId: string(),
    assetId: string(),
    assetName: string(),
    assetType: enumeration<
      'image' | 'video' | 'document' | 'template' | 'landing_page'
    >(),
    fileUrl: string(),
    thumbnailUrl: string().optional(),
    tags: json<readonly string[]>(),
    usageCount: number(),
    performanceMetrics: json<{
      readonly views?: number;
      readonly clicks?: number;
      readonly conversions?: number;
    }>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'assetId');

const formSubmission = table('formSubmission')
  .columns({
    workspaceId: string(),
    submissionId: string(),
    formId: string(),
    submitterEmail: string(),
    submitterName: string().optional(),
    fieldValues: json<{
      readonly [fieldName: string]: string | number | boolean;
    }>(),
    submissionDate: string(),
    sourceUrl: string(),
    leadCreated: boolean(),
    leadId: string().optional(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'submissionId');

const jobPosting = table('jobPosting')
  .columns({
    workspaceId: string(),
    jobId: string(),
    jobTitle: string(),
    description: string(),
    descriptionPlain: string(),
    department: string(),
    location: string(),
    locationType: enumeration<'onsite' | 'remote' | 'hybrid'>(),
    employmentType: enumeration<
      'full_time' | 'part_time' | 'contract' | 'intern'
    >(),
    salaryRange: json<{
      readonly min: number;
      readonly max: number;
      readonly currency: string;
    }>().optional(),
    requirements: json<readonly string[]>(),
    responsibilities: json<readonly string[]>(),
    benefits: json<readonly string[]>(),
    status: enumeration<'draft' | 'published' | 'closed' | 'filled'>(),
    requisitionNumber: string().optional(),
    hiringManagerId: string(),
    recruiterId: string().optional(),
    openings: number(),
    publishedAt: string().optional(),
    closedAt: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'jobId');

const candidate = table('candidate')
  .columns({
    workspaceId: string(),
    candidateId: string(),
    firstName: string(),
    lastName: string(),
    email: string(),
    phone: string().optional(),
    resumeUrl: string(),
    coverLetterUrl: string().optional(),
    linkedinUrl: string().optional(),
    portfolioUrl: string().optional(),
    skills: json<readonly string[]>(),
    experienceYears: number(),
    education: json<
      readonly {
        readonly degree: string;
        readonly school: string;
        readonly year: number;
      }[]
    >(),
    source: enumeration<
      | 'job_board'
      | 'referral'
      | 'company_website'
      | 'recruiter'
      | 'linkedin'
      | 'other'
    >(),
    referredBy: string().optional(),
    pipelineStage: enumeration<
      'applied' | 'screening' | 'interview' | 'offer' | 'hired' | 'rejected'
    >(),
    rating: number().optional(),
    tags: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'candidateId');

const application = table('application')
  .columns({
    workspaceId: string(),
    applicationId: string(),
    jobId: string(),
    candidateId: string(),
    appliedDate: string(),
    status: enumeration<
      | 'applied'
      | 'reviewing'
      | 'screening'
      | 'interviewing'
      | 'offered'
      | 'hired'
      | 'rejected'
      | 'withdrawn'
    >(),
    screeningAnswers: json<{
      readonly [question: string]: string;
    }>().optional(),
    recruiterNotes: string().optional(),
    rejectionReason: string().optional(),
    source: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'applicationId');

const interview = table('interview')
  .columns({
    workspaceId: string(),
    interviewId: string(),
    applicationId: string(),
    candidateId: string(),
    interviewType: enumeration<
      'phone_screen' | 'video' | 'onsite' | 'technical' | 'panel' | 'behavioral'
    >(),
    scheduledTime: string(),
    duration: number(),
    interviewerIds: json<readonly string[]>(),
    location: string().optional(),
    meetingLink: string().optional(),
    status: enumeration<'scheduled' | 'completed' | 'cancelled' | 'no_show'>(),
    feedback: string().optional(),
    outcome: enumeration<'pass' | 'fail' | 'maybe'>().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'interviewId');

const interviewFeedback = table('interviewFeedback')
  .columns({
    workspaceId: string(),
    feedbackId: string(),
    interviewId: string(),
    interviewerId: string(),
    ratings: json<{readonly [criterion: string]: number}>(),
    strengths: json<readonly string[]>(),
    concerns: json<readonly string[]>(),
    recommendation: enumeration<
      'strong_yes' | 'yes' | 'maybe' | 'no' | 'strong_no'
    >(),
    notes: string(),
    submittedAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'feedbackId');

const jobOffer = table('jobOffer')
  .columns({
    workspaceId: string(),
    offerId: string(),
    applicationId: string(),
    candidateId: string(),
    offerDate: string(),
    salary: number(),
    equityOptions: number().optional(),
    benefits: json<readonly string[]>(),
    startDate: number(),
    expirationDate: string(),
    acceptanceStatus: enumeration<
      'pending' | 'accepted' | 'declined' | 'expired'
    >(),
    acceptedAt: string().optional(),
    signedContractUrl: string().optional(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'offerId');

const candidateNote = table('candidateNote')
  .columns({
    workspaceId: string(),
    noteId: string(),
    candidateId: string(),
    authorId: string(),
    noteContent: string(),
    visibility: enumeration<'private' | 'team' | 'hiring_manager'>(),
    isPinned: boolean(),
    tags: json<readonly string[]>(),
    attachments: json<readonly string[]>().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'noteId');

const hiringPipeline = table('hiringPipeline')
  .columns({
    workspaceId: string(),
    pipelineId: string(),
    pipelineName: string(),
    stages: json<
      readonly {
        readonly stageId: string;
        readonly name: string;
        readonly order: number;
        readonly durationDays: number;
      }[]
    >(),
    jobTypes: json<readonly string[]>(),
    slas: json<{readonly [stage: string]: number}>(),
    automationRules:
      json<readonly {readonly trigger: string; readonly action: string}[]>(),
    conversionMetrics: json<{readonly [stage: string]: number}>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'pipelineId');

const onboardingTask = table('onboardingTask')
  .columns({
    workspaceId: string(),
    taskId: string(),
    offerId: string(),
    candidateId: string(),
    taskName: string(),
    description: string(),
    assigneeId: string().optional(),
    dueDate: string(),
    completionStatus: enumeration<'pending' | 'in_progress' | 'completed'>(),
    completedAt: string().optional(),
    dependencies: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'taskId');

const recruiterAssignment = table('recruiterAssignment')
  .columns({
    workspaceId: string(),
    assignmentId: string(),
    jobId: string(),
    recruiterId: string(),
    role: enumeration<'lead' | 'support'>(),
    workloadPercentage: number(),
    activeSince: string(),
    placementCount: number(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'assignmentId');

const event = table('event')
  .columns({
    workspaceId: string(),
    eventId: string(),
    eventName: string(),
    description: string(),
    eventType: enumeration<
      | 'conference'
      | 'workshop'
      | 'webinar'
      | 'networking'
      | 'concert'
      | 'sports'
      | 'other'
    >(),
    startTime: string(),
    endTime: string(),
    venueId: string().optional(),
    capacity: number(),
    registrationSettings: json<{
      readonly openDate: string;
      readonly closeDate: string;
      readonly requiresApproval: boolean;
      readonly maxPerRegistration: number;
    }>(),
    status: enumeration<
      | 'draft'
      | 'published'
      | 'registration_open'
      | 'registration_closed'
      | 'in_progress'
      | 'completed'
      | 'cancelled'
    >(),
    visibility: enumeration<'public' | 'private' | 'invite_only'>(),
    tags: json<readonly string[]>(),
    createdById: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'eventId');

const venue = table('venue')
  .columns({
    workspaceId: string(),
    venueId: string(),
    venueName: string(),
    address: string(),
    city: string(),
    state: string(),
    zip: string(),
    country: string(),
    capacity: number(),
    roomConfigurations: json<
      readonly {
        readonly name: string;
        readonly capacity: number;
        readonly setup: string;
      }[]
    >(),
    amenities: json<readonly string[]>(),
    contactInfo: json<{
      readonly name: string;
      readonly phone: string;
      readonly email: string;
    }>(),
    availabilityCalendar: string().optional(),
    costPerDay: number().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'venueId');

const ticketType = table('ticketType')
  .columns({
    workspaceId: string(),
    ticketTypeId: string(),
    eventId: string(),
    ticketName: string(),
    description: string().optional(),
    price: number(),
    quantity: number(),
    quantitySold: number(),
    salesStartDate: string(),
    salesEndDate: string(),
    accessLevel: enumeration<'general' | 'vip' | 'early_bird' | 'student'>(),
    perks: json<readonly string[]>(),
    status: enumeration<'on_sale' | 'sold_out' | 'ended'>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'ticketTypeId');

const ticketPurchase = table('ticketPurchase')
  .columns({
    workspaceId: string(),
    purchaseId: string(),
    eventId: string(),
    buyerName: string(),
    buyerEmail: string(),
    buyerPhone: string().optional(),
    ticketTypes: json<
      readonly {
        readonly ticketTypeId: string;
        readonly quantity: number;
        readonly price: number;
      }[]
    >(),
    totalAmount: number(),
    fees: number(),
    paymentStatus: enumeration<
      'pending' | 'completed' | 'failed' | 'refunded'
    >(),
    orderNumber: string(),
    paymentMethod: string().optional(),
    purchasedAt: string(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'purchaseId');

const attendee = table('attendee')
  .columns({
    workspaceId: string(),
    attendeeId: string(),
    ticketPurchaseId: string(),
    eventId: string(),
    firstName: string(),
    lastName: string(),
    email: string(),
    phone: string().optional(),
    checkInStatus: boolean(),
    checkInTime: string().optional(),
    badgePrinted: boolean(),
    dietaryRestrictions: json<readonly string[]>().optional(),
    sessionRegistrations: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'attendeeId');

const eventSession = table('eventSession')
  .columns({
    workspaceId: string(),
    sessionId: string(),
    eventId: string(),
    sessionTitle: string(),
    description: string(),
    startTime: string(),
    endTime: string(),
    location: string(),
    speakerIds: json<readonly string[]>(),
    capacity: number(),
    registrationRequired: boolean(),
    registeredCount: number(),
    tags: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'sessionId');

const speaker = table('speaker')
  .columns({
    workspaceId: string(),
    speakerId: string(),
    speakerName: string(),
    bio: string(),
    photoUrl: string().optional(),
    company: string().optional(),
    title: string().optional(),
    socialLinks:
      json<readonly {readonly platform: string; readonly url: string}[]>(),
    topics: json<readonly string[]>(),
    sessionsAssigned: json<readonly string[]>(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'speakerId');

const sponsor = table('sponsor')
  .columns({
    workspaceId: string(),
    sponsorId: string(),
    eventId: string(),
    sponsorName: string(),
    tierLevel: enumeration<'platinum' | 'gold' | 'silver' | 'bronze'>(),
    logoUrl: string(),
    website: string().optional(),
    boothNumber: string().optional(),
    benefits: json<readonly string[]>(),
    contractValue: number(),
    materials:
      json<
        readonly {readonly type: string; readonly url: string}[]
      >().optional(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'sponsorId');

const sessionRegistration = table('sessionRegistration')
  .columns({
    workspaceId: string(),
    registrationId: string(),
    sessionId: string(),
    attendeeId: string(),
    registeredAt: string(),
    waitlistPosition: number().optional(),
    attendanceConfirmed: boolean(),
    createdAt: number(),
  })
  .primaryKey('workspaceId', 'registrationId');

const eventSchedule = table('eventSchedule')
  .columns({
    workspaceId: string(),
    scheduleId: string(),
    eventId: string(),
    timeSlots: json<
      readonly {
        readonly time: string;
        readonly sessionIds: readonly string[];
        readonly type: string;
      }[]
    >(),
    tracks:
      json<readonly {readonly trackName: string; readonly color: string}[]>(),
    sessionAssignments: json<{
      readonly [sessionId: string]: {
        readonly track: string;
        readonly room: string;
      };
    }>(),
    breaks: json<
      readonly {
        readonly time: string;
        readonly duration: number;
        readonly type: string;
      }[]
    >(),
    specialEvents: json<
      readonly {
        readonly time: string;
        readonly name: string;
        readonly location: string;
      }[]
    >(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey('workspaceId', 'scheduleId');

// ==================== RELATIONSHIP DEFINITIONS ====================

const userRelationships = relationships(user, ({many}) => ({
  sessions: many({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: session,
  }),
  accounts: many({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: account,
  }),
  workspaceMembers: many({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: workspaceMember,
  }),
  invitedWorkspaceMembers: many({
    sourceField: ['userId'],
    destField: ['invitedById'],
    destSchema: workspaceMember,
  }),
  ledTeams: many({
    sourceField: ['userId'],
    destField: ['leaderId'],
    destSchema: team,
  }),
  createdTeams: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: team,
  }),
  apiKeys: many({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: apiKey,
  }),
  createdApiKeys: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: apiKey,
  }),
  passwordResets: many({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: passwordReset,
  }),
  auditLogs: many({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: auditLog,
  }),
  createdEntityTags: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: entityTag,
  }),
  uploadedEntityAttachments: many({
    sourceField: ['userId'],
    destField: ['uploadedById'],
    destSchema: entityAttachment,
  }),
  createdEntityComments: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: entityComment,
  }),
  updatedEntityComments: many({
    sourceField: ['userId'],
    destField: ['updatedById'],
    destSchema: entityComment,
  }),
  createdCustomFieldDefinitions: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: customFieldDefinition,
  }),
  updatedCustomFieldValues: many({
    sourceField: ['userId'],
    destField: ['updatedById'],
    destSchema: customFieldValue,
  }),
  createdWebhooks: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: webhook,
  }),
  createdFeatureFlags: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: featureFlag,
  }),
  createdIntegrations: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: integration,
  }),
  createdEmailCampaigns: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: emailCampaign,
  }),
  createdEmailTemplates: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: emailTemplate,
  }),
  createdSupportTickets: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: supportTicket,
  }),
  assignedSupportTickets: many({
    sourceField: ['userId'],
    destField: ['assignedToId'],
    destSchema: supportTicket,
  }),
  createdProducts: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: product,
  }),
  createdCmsArticles: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: cmsArticle,
  }),
  updatedCmsArticles: many({
    sourceField: ['userId'],
    destField: ['updatedById'],
    destSchema: cmsArticle,
  }),
  createdCmsPages: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: cmsPage,
  }),
  updatedCmsPages: many({
    sourceField: ['userId'],
    destField: ['updatedById'],
    destSchema: cmsPage,
  }),
  uploadedMediaAssets: many({
    sourceField: ['userId'],
    destField: ['uploadedById'],
    destSchema: mediaAsset,
  }),
  createdProjects: many({
    sourceField: ['userId'],
    destField: ['createdById'],
    destSchema: project,
  }),
  ownedProjects: many({
    sourceField: ['userId'],
    destField: ['ownerId'],
    destSchema: project,
  }),
}));

const workspaceRelationships = relationships(workspace, ({many}) => ({
  sessions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: session,
  }),
  accounts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: account,
  }),
  workspaceMembers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspaceMember,
  }),
  teams: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: team,
  }),
  apiKeys: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: apiKey,
  }),
  verificationTokens: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: verificationToken,
  }),
  passwordResets: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: passwordReset,
  }),
  auditLogs: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: auditLog,
  }),
  entityTags: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: entityTag,
  }),
  entityAttachments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: entityAttachment,
  }),
  entityComments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: entityComment,
  }),
  customFieldDefinitions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: customFieldDefinition,
  }),
  customFieldValues: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: customFieldValue,
  }),
  webhooks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: webhook,
  }),
  webhookDeliveries: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: webhookDelivery,
  }),
  rateLimits: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: rateLimit,
  }),
  featureFlags: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: featureFlag,
  }),
  integrations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: integration,
  }),
  emailCampaigns: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: emailCampaign,
  }),
  emailTemplates: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: emailTemplate,
  }),
  emailSends: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: emailSend,
  }),
  subscriberLists: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: subscriberList,
  }),
  subscribers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: subscriber,
  }),
  automationWorkflows: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: automationWorkflow,
  }),
  emailLinks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: emailLink,
  }),
  unsubscribeEvents: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: unsubscribeEvent,
  }),
  emailAttachments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: emailAttachment,
  }),
  spamComplaints: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: spamComplaint,
  }),
  supportTickets: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: supportTicket,
  }),
  ticketMessages: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ticketMessage,
  }),
  knowledgeBaseArticles: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: knowledgeBaseArticle,
  }),
  slaPolicies: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: slaPolicy,
  }),
  cannedResponses: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: cannedResponse,
  }),
  ticketTags: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ticketTag,
  }),
  satisfactionSurveys: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: satisfactionSurvey,
  }),
  agentAssignments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: agentAssignment,
  }),
  ticketEscalations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ticketEscalation,
  }),
  ticketMerges: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ticketMerge,
  }),
  products: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: product,
  }),
  productVariants: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: productVariant,
  }),
  orders: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: order,
  }),
  orderLineItems: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: orderLineItem,
  }),
  shoppingCarts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: shoppingCart,
  }),
  productReviews: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: productReview,
  }),
  inventoryAdjustments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: inventoryAdjustment,
  }),
  discountCodes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: discountCode,
  }),
  shippingZones: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: shippingZone,
  }),
  paymentTransactions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: paymentTransaction,
  }),
  cmsArticles: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: cmsArticle,
  }),
  cmsPages: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: cmsPage,
  }),
  mediaAssets: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: mediaAsset,
  }),
  contentRevisions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: contentRevision,
  }),
  taxonomyTerms: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: taxonomyTerm,
  }),
  contentBlocks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: contentBlock,
  }),
  cmsMenus: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: cmsMenu,
  }),
  redirectRules: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: redirectRule,
  }),
  cmsComments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: cmsComment,
  }),
  contentLocks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: contentLock,
  }),
  projects: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: project,
  }),
  tasks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: task,
  }),
  taskDependencies: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: taskDependency,
  }),
  sprints: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: sprint,
  }),
  boards: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: board,
  }),
  timeEntries: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: timeEntry,
  }),
  taskComments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: taskComment,
  }),
  milestones: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: milestone,
  }),
  projectBudgets: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: projectBudget,
  }),
  resourceAllocations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: resourceAllocation,
  }),
  employees: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: employee,
  }),
  payrollRuns: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: payrollRun,
  }),
  payrollLines: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: payrollLine,
  }),
  timeOffRequests: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: timeOffRequest,
  }),
  benefitsEnrollments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: benefitsEnrollment,
  }),
  performanceReviews: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: performanceReview,
  }),
  departments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: department,
  }),
  compensationChanges: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: compensationChange,
  }),
  trainingRecords: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: trainingRecord,
  }),
  attendanceLogs: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: attendanceLog,
  }),
  ledgerAccounts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ledgerAccount,
  }),
  journalEntries: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: journalEntry,
  }),
  journalLines: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: journalLine,
  }),
  invoices: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: invoice,
  }),
  payments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: payment,
  }),
  bankTransactions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: bankTransaction,
  }),
  expenseClaims: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: expenseClaim,
  }),
  budgets: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: budget,
  }),
  taxRates: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: taxRate,
  }),
  reconciliations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: reconciliation,
  }),
  patients: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: patient,
  }),
  appointments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: appointment,
  }),
  medicalRecords: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: medicalRecord,
  }),
  prescriptions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: prescription,
  }),
  labOrders: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: labOrder,
  }),
  labResults: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: labResult,
  }),
  insuranceClaims: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: insuranceClaim,
  }),
  diagnosis: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: diagnosis,
  }),
  vitalSigns: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: vitalSigns,
  }),
  immunizations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: immunization,
  }),
  properties: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: property,
  }),
  listings: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: listing,
  }),
  showings: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: showing,
  }),
  offers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: offer,
  }),
  contracts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: contract,
  }),
  propertyDocuments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: propertyDocument,
  }),
  buyerProfiles: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: buyerProfile,
  }),
  marketAnalysiss: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: marketAnalysis,
  }),
  inspectionReports: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: inspectionReport,
  }),
  commissionSplits: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: commissionSplit,
  }),
  courses: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: course,
  }),
  courseModules: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: courseModule,
  }),
  lessons: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: lesson,
  }),
  enrollments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: enrollment,
  }),
  assignments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: assignment,
  }),
  submissions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: submission,
  }),
  quizzes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: quiz,
  }),
  quizAttempts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: quizAttempt,
  }),
  discussionThreads: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: discussionThread,
  }),
  certificates: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: certificate,
  }),
  videos: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: video,
  }),
  videoMetadata: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: videoMetadata,
  }),
  playlists: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: playlist,
  }),
  subscriptions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: subscription,
  }),
  watchHistories: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: watchHistory,
  }),
  videoComments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: videoComment,
  }),
  channels: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: channel,
  }),
  videoAnalytics: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: videoAnalytics,
  }),
  livestreams: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: livestream,
  }),
  contentRestrictions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: contentRestriction,
  }),
  profiles: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: profile,
  }),
  posts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: post,
  }),
  postMedias: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: postMedia,
  }),
  followRelationships: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: followRelationship,
  }),
  likes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: like,
  }),
  comments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: comment,
  }),
  directMessages: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: directMessage,
  }),
  dmConversations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: dmConversation,
  }),
  notifications: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: notification,
  }),
  blockRelationships: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: blockRelationship,
  }),
  legalCases: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: legalCase,
  }),
  caseParties: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: caseParty,
  }),
  legalDocuments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: legalDocument,
  }),
  courtDates: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: courtDate,
  }),
  billableTimes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: billableTime,
  }),
  matters: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: matter,
  }),
  trustTransactions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: trustTransaction,
  }),
  conflictChecks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: conflictCheck,
  }),
  retainerAgreements: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: retainerAgreement,
  }),
  legalResearches: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: legalResearch,
  }),
  menus: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: menu,
  }),
  menuItems: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: menuItem,
  }),
  ingredients: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ingredient,
  }),
  recipes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: recipe,
  }),
  restaurantOrders: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: restaurantOrder,
  }),
  restaurantOrderItems: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: restaurantOrderItem,
  }),
  restaurantReservations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: restaurantReservation,
  }),
  suppliers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: supplier,
  }),
  purchaseOrders: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: purchaseOrder,
  }),
  inventoryCounts: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: inventoryCount,
  }),
  shipments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: shipment,
  }),
  shipmentItems: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: shipmentItem,
  }),
  routes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: route,
  }),
  trackingEvents: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: trackingEvent,
  }),
  drivers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: driver,
  }),
  vehicles: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: vehicle,
  }),
  warehouses: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: warehouse,
  }),
  warehouseZones: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: warehouseZone,
  }),
  deliveryManifests: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: deliveryManifest,
  }),
  freightInvoices: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: freightInvoice,
  }),
  policies: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: policy,
  }),
  policyCoverages: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: policyCoverage,
  }),
  claims: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: claim,
  }),
  claimPayments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: claimPayment,
  }),
  underwritingReviews: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: underwritingReview,
  }),
  policyDocuments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: policyDocument,
  }),
  premiumPayments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: premiumPayment,
  }),
  adjusterAssignments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: adjusterAssignment,
  }),
  lossHistories: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: lossHistory,
  }),
  reinsuranceTreaties: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: reinsuranceTreaty,
  }),
  rooms: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: room,
  }),
  reservations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: reservation,
  }),
  guests: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: guest,
  }),
  ratePlans: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ratePlan,
  }),
  housekeepingTasks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: housekeepingTask,
  }),
  guestServiceRequests: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: guestServiceRequest,
  }),
  amenityBookings: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: amenityBooking,
  }),
  roomRateOverrides: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: roomRateOverride,
  }),
  folioCharges: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: folioCharge,
  }),
  maintenanceIssues: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: maintenanceIssue,
  }),
  marketingCampaigns: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: marketingCampaign,
  }),
  leads: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: lead,
  }),
  segments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: segment,
  }),
  customerJourneys: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: customerJourney,
  }),
  abTests: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: abTest,
  }),
  leadActivities: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: leadActivity,
  }),
  attributionTouchpoints: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: attributionTouchpoint,
  }),
  conversionGoals: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: conversionGoal,
  }),
  marketingAssets: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: marketingAsset,
  }),
  formSubmissions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: formSubmission,
  }),
  jobPostings: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: jobPosting,
  }),
  candidates: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: candidate,
  }),
  applications: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: application,
  }),
  interviews: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: interview,
  }),
  interviewFeedbacks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: interviewFeedback,
  }),
  jobOffers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: jobOffer,
  }),
  candidateNotes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: candidateNote,
  }),
  hiringPipelines: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: hiringPipeline,
  }),
  onboardingTasks: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: onboardingTask,
  }),
  recruiterAssignments: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: recruiterAssignment,
  }),
  events: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: event,
  }),
  venues: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: venue,
  }),
  ticketTypes: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ticketType,
  }),
  ticketPurchases: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: ticketPurchase,
  }),
  attendees: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: attendee,
  }),
  eventSessions: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: eventSession,
  }),
  speakers: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: speaker,
  }),
  sponsors: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: sponsor,
  }),
  sessionRegistrations: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: sessionRegistration,
  }),
  eventSchedules: many({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: eventSchedule,
  }),
}));

const sessionRelationships = relationships(session, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const accountRelationships = relationships(account, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const workspaceMemberRelationships = relationships(
  workspaceMember,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    user: one({
      sourceField: ['userId'],
      destField: ['userId'],
      destSchema: user,
    }),
    invitedByUser: one({
      sourceField: ['invitedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const teamRelationships = relationships(team, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  parentTeam: one({
    sourceField: ['parentTeamId'],
    destField: ['teamId'],
    destSchema: team,
  }),
  leader: one({
    sourceField: ['leaderId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const apiKeyRelationships = relationships(apiKey, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const verificationTokenRelationships = relationships(
  verificationToken,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
  }),
);

const passwordResetRelationships = relationships(passwordReset, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const auditLogRelationships = relationships(auditLog, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== UNIVERSAL/JUNCTION TABLES ====================

const entityTagRelationships = relationships(entityTag, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const entityAttachmentRelationships = relationships(
  entityAttachment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    uploadedByUser: one({
      sourceField: ['uploadedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const entityCommentRelationships = relationships(entityComment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  parentComment: one({
    sourceField: ['parentCommentId'],
    destField: ['commentId'],
    destSchema: entityComment,
  }),
}));

const customFieldDefinitionRelationships = relationships(
  customFieldDefinition,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const customFieldValueRelationships = relationships(
  customFieldValue,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    fieldDefinition: one({
      sourceField: ['fieldId'],
      destField: ['fieldId'],
      destSchema: customFieldDefinition,
    }),
    updatedByUser: one({
      sourceField: ['updatedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const webhookRelationships = relationships(webhook, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const webhookDeliveryRelationships = relationships(
  webhookDelivery,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    webhook: one({
      sourceField: ['webhookId'],
      destField: ['webhookId'],
      destSchema: webhook,
    }),
  }),
);

const rateLimitRelationships = relationships(rateLimit, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const featureFlagRelationships = relationships(featureFlag, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const integrationRelationships = relationships(integration, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== EMAIL MARKETING PLATFORM ====================

const emailCampaignRelationships = relationships(emailCampaign, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  template: one({
    sourceField: ['templateId'],
    destField: ['templateId'],
    destSchema: emailTemplate,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const emailTemplateRelationships = relationships(emailTemplate, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  approvedByUser: one({
    sourceField: ['approvedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const emailSendRelationships = relationships(emailSend, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  campaign: one({
    sourceField: ['campaignId'],
    destField: ['campaignId'],
    destSchema: emailCampaign,
  }),
  subscriber: one({
    sourceField: ['subscriberId'],
    destField: ['subscriberId'],
    destSchema: subscriber,
  }),
}));

const subscriberListRelationships = relationships(subscriberList, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const subscriberRelationships = relationships(subscriber, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const automationWorkflowRelationships = relationships(
  automationWorkflow,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
    updatedByUser: one({
      sourceField: ['updatedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const emailLinkRelationships = relationships(emailLink, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  send: one({
    sourceField: ['sendId'],
    destField: ['sendId'],
    destSchema: emailSend,
  }),
}));

const unsubscribeEventRelationships = relationships(
  unsubscribeEvent,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    subscriber: one({
      sourceField: ['subscriberId'],
      destField: ['subscriberId'],
      destSchema: subscriber,
    }),
    campaign: one({
      sourceField: ['campaignId'],
      destField: ['campaignId'],
      destSchema: emailCampaign,
    }),
  }),
);

const emailAttachmentRelationships = relationships(
  emailAttachment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    uploadedByUser: one({
      sourceField: ['uploadedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const spamComplaintRelationships = relationships(spamComplaint, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  send: one({
    sourceField: ['sendId'],
    destField: ['sendId'],
    destSchema: emailSend,
  }),
  subscriber: one({
    sourceField: ['subscriberId'],
    destField: ['subscriberId'],
    destSchema: subscriber,
  }),
  resolvedByUser: one({
    sourceField: ['resolvedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== CUSTOMER SUPPORT / HELPDESK ====================

const supportTicketRelationships = relationships(supportTicket, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  assignedToUser: one({
    sourceField: ['assignedToId'],
    destField: ['userId'],
    destSchema: user,
  }),
  team: one({
    sourceField: ['teamId'],
    destField: ['teamId'],
    destSchema: team,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const ticketMessageRelationships = relationships(ticketMessage, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  ticket: one({
    sourceField: ['ticketId'],
    destField: ['ticketId'],
    destSchema: supportTicket,
  }),
  senderUser: one({
    sourceField: ['senderUserId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const knowledgeBaseArticleRelationships = relationships(
  knowledgeBaseArticle,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    author: one({
      sourceField: ['authorId'],
      destField: ['userId'],
      destSchema: user,
    }),
    reviewedByUser: one({
      sourceField: ['reviewedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const slaPolicyRelationships = relationships(slaPolicy, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const cannedResponseRelationships = relationships(cannedResponse, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  lastUsedByUser: one({
    sourceField: ['lastUsedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const ticketTagRelationships = relationships(ticketTag, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const satisfactionSurveyRelationships = relationships(
  satisfactionSurvey,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    ticket: one({
      sourceField: ['ticketId'],
      destField: ['ticketId'],
      destSchema: supportTicket,
    }),
  }),
);

const agentAssignmentRelationships = relationships(
  agentAssignment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    ticket: one({
      sourceField: ['ticketId'],
      destField: ['ticketId'],
      destSchema: supportTicket,
    }),
    agentUser: one({
      sourceField: ['agentUserId'],
      destField: ['userId'],
      destSchema: user,
    }),
    assignedByUser: one({
      sourceField: ['assignedById'],
      destField: ['userId'],
      destSchema: user,
    }),
    team: one({
      sourceField: ['teamId'],
      destField: ['teamId'],
      destSchema: team,
    }),
  }),
);

const ticketEscalationRelationships = relationships(
  ticketEscalation,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    ticket: one({
      sourceField: ['ticketId'],
      destField: ['ticketId'],
      destSchema: supportTicket,
    }),
    escalatedToUser: one({
      sourceField: ['escalatedToUserId'],
      destField: ['userId'],
      destSchema: user,
    }),
    escalatedFromUser: one({
      sourceField: ['escalatedFromUserId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const ticketMergeRelationships = relationships(ticketMerge, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  primaryTicket: one({
    sourceField: ['primaryTicketId'],
    destField: ['ticketId'],
    destSchema: supportTicket,
  }),
  mergedByUser: one({
    sourceField: ['mergedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== E-COMMERCE PLATFORM ====================

const productRelationships = relationships(product, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const productVariantRelationships = relationships(productVariant, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  product: one({
    sourceField: ['productId'],
    destField: ['productId'],
    destSchema: product,
  }),
}));

const orderRelationships = relationships(order, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const orderLineItemRelationships = relationships(orderLineItem, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  order: one({
    sourceField: ['orderId'],
    destField: ['orderId'],
    destSchema: order,
  }),
  product: one({
    sourceField: ['productId'],
    destField: ['productId'],
    destSchema: product,
  }),
  variant: one({
    sourceField: ['variantId'],
    destField: ['variantId'],
    destSchema: productVariant,
  }),
}));

const shoppingCartRelationships = relationships(shoppingCart, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const productReviewRelationships = relationships(productReview, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  product: one({
    sourceField: ['productId'],
    destField: ['productId'],
    destSchema: product,
  }),
}));

const inventoryAdjustmentRelationships = relationships(
  inventoryAdjustment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    product: one({
      sourceField: ['productId'],
      destField: ['productId'],
      destSchema: product,
    }),
    variant: one({
      sourceField: ['variantId'],
      destField: ['variantId'],
      destSchema: productVariant,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const discountCodeRelationships = relationships(discountCode, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const shippingZoneRelationships = relationships(shippingZone, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const paymentTransactionRelationships = relationships(
  paymentTransaction,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    order: one({
      sourceField: ['orderId'],
      destField: ['orderId'],
      destSchema: order,
    }),
  }),
);

// ==================== CONTENT MANAGEMENT SYSTEM ====================

const cmsArticleRelationships = relationships(cmsArticle, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['userId'],
    destSchema: user,
  }),
  lastModifiedByUser: one({
    sourceField: ['lastModifiedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const cmsPageRelationships = relationships(cmsPage, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  parentPage: one({
    sourceField: ['parentPageId'],
    destField: ['pageId'],
    destSchema: cmsPage,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const mediaAssetRelationships = relationships(mediaAsset, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  uploadedByUser: one({
    sourceField: ['uploadedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const contentRevisionRelationships = relationships(
  contentRevision,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    author: one({
      sourceField: ['authorId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const taxonomyTermRelationships = relationships(taxonomyTerm, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  parentTerm: one({
    sourceField: ['parentTermId'],
    destField: ['termId'],
    destSchema: taxonomyTerm,
  }),
}));

const contentBlockRelationships = relationships(contentBlock, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const cmsMenuRelationships = relationships(cmsMenu, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const redirectRuleRelationships = relationships(redirectRule, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const cmsCommentRelationships = relationships(cmsComment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  article: one({
    sourceField: ['articleId'],
    destField: ['articleId'],
    destSchema: cmsArticle,
  }),
  parentComment: one({
    sourceField: ['parentCommentId'],
    destField: ['commentId'],
    destSchema: cmsComment,
  }),
}));

const contentLockRelationships = relationships(contentLock, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  lockedByUser: one({
    sourceField: ['lockedByUserId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== PROJECT MANAGEMENT ====================

const projectRelationships = relationships(project, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  owner: one({
    sourceField: ['ownerId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const taskRelationships = relationships(task, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
  parentTask: one({
    sourceField: ['parentTaskId'],
    destField: ['taskId'],
    destSchema: task,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const taskDependencyRelationships = relationships(taskDependency, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  predecessorTask: one({
    sourceField: ['predecessorTaskId'],
    destField: ['taskId'],
    destSchema: task,
  }),
  successorTask: one({
    sourceField: ['successorTaskId'],
    destField: ['taskId'],
    destSchema: task,
  }),
}));

const sprintRelationships = relationships(sprint, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const boardRelationships = relationships(board, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const timeEntryRelationships = relationships(timeEntry, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  task: one({
    sourceField: ['taskId'],
    destField: ['taskId'],
    destSchema: task,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
  approvedByUser: one({
    sourceField: ['approvedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
}));

const taskCommentRelationships = relationships(taskComment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  task: one({
    sourceField: ['taskId'],
    destField: ['taskId'],
    destSchema: task,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const milestoneRelationships = relationships(milestone, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const projectBudgetRelationships = relationships(projectBudget, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
}));

const resourceAllocationRelationships = relationships(
  resourceAllocation,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    project: one({
      sourceField: ['projectId'],
      destField: ['projectId'],
      destSchema: project,
    }),
    user: one({
      sourceField: ['userId'],
      destField: ['userId'],
      destSchema: user,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

// ==================== HR/PAYROLL SYSTEM ====================

const employeeRelationships = relationships(employee, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  department: one({
    sourceField: ['departmentId'],
    destField: ['departmentId'],
    destSchema: department,
  }),
  manager: one({
    sourceField: ['managerId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
}));

const payrollRunRelationships = relationships(payrollRun, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  approvedByUser: one({
    sourceField: ['approvedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const payrollLineRelationships = relationships(payrollLine, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  run: one({
    sourceField: ['runId'],
    destField: ['runId'],
    destSchema: payrollRun,
  }),
  employee: one({
    sourceField: ['employeeId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
}));

const timeOffRequestRelationships = relationships(timeOffRequest, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  employee: one({
    sourceField: ['employeeId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
  approver: one({
    sourceField: ['approverId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const benefitsEnrollmentRelationships = relationships(
  benefitsEnrollment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    employee: one({
      sourceField: ['employeeId'],
      destField: ['employeeId'],
      destSchema: employee,
    }),
  }),
);

const performanceReviewRelationships = relationships(
  performanceReview,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    employee: one({
      sourceField: ['employeeId'],
      destField: ['employeeId'],
      destSchema: employee,
    }),
    reviewer: one({
      sourceField: ['reviewerId'],
      destField: ['employeeId'],
      destSchema: employee,
    }),
  }),
);

const departmentRelationships = relationships(department, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  parentDepartment: one({
    sourceField: ['parentDepartmentId'],
    destField: ['departmentId'],
    destSchema: department,
  }),
  headOfDepartment: one({
    sourceField: ['headOfDepartmentId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
}));

const compensationChangeRelationships = relationships(
  compensationChange,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    employee: one({
      sourceField: ['employeeId'],
      destField: ['employeeId'],
      destSchema: employee,
    }),
    approver: one({
      sourceField: ['approverId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const trainingRecordRelationships = relationships(trainingRecord, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  employee: one({
    sourceField: ['employeeId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
}));

const attendanceLogRelationships = relationships(attendanceLog, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  employee: one({
    sourceField: ['employeeId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
}));

// ==================== ACCOUNTING/FINANCE ====================

const ledgerAccountRelationships = relationships(ledgerAccount, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  parentAccount: one({
    sourceField: ['parentAccountId'],
    destField: ['accountId'],
    destSchema: ledgerAccount,
  }),
}));

const journalEntryRelationships = relationships(journalEntry, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  postedByUser: one({
    sourceField: ['postedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  approvedByUser: one({
    sourceField: ['approvedById'],
    destField: ['userId'],
    destSchema: user,
  }),
  reversalEntry: one({
    sourceField: ['reversalEntryId'],
    destField: ['entryId'],
    destSchema: journalEntry,
  }),
}));

const journalLineRelationships = relationships(journalLine, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  entry: one({
    sourceField: ['entryId'],
    destField: ['entryId'],
    destSchema: journalEntry,
  }),
  account: one({
    sourceField: ['accountId'],
    destField: ['accountId'],
    destSchema: ledgerAccount,
  }),
}));

const invoiceRelationships = relationships(invoice, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  project: one({
    sourceField: ['projectId'],
    destField: ['projectId'],
    destSchema: project,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const paymentRelationships = relationships(payment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const bankTransactionRelationships = relationships(
  bankTransaction,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    account: one({
      sourceField: ['accountId'],
      destField: ['accountId'],
      destSchema: ledgerAccount,
    }),
  }),
);

const expenseClaimRelationships = relationships(expenseClaim, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  employee: one({
    sourceField: ['employeeId'],
    destField: ['employeeId'],
    destSchema: employee,
  }),
}));

const budgetRelationships = relationships(budget, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  department: one({
    sourceField: ['departmentId'],
    destField: ['departmentId'],
    destSchema: department,
  }),
  account: one({
    sourceField: ['accountId'],
    destField: ['accountId'],
    destSchema: ledgerAccount,
  }),
}));

const taxRateRelationships = relationships(taxRate, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const reconciliationRelationships = relationships(reconciliation, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  account: one({
    sourceField: ['accountId'],
    destField: ['accountId'],
    destSchema: ledgerAccount,
  }),
  reconciledByUser: one({
    sourceField: ['reconciledById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== HEALTHCARE/EMR ====================

const patientRelationships = relationships(patient, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  primaryPhysician: one({
    sourceField: ['primaryPhysicianId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const appointmentRelationships = relationships(appointment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  provider: one({
    sourceField: ['providerId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const medicalRecordRelationships = relationships(medicalRecord, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  provider: one({
    sourceField: ['providerId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const prescriptionRelationships = relationships(prescription, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  prescriber: one({
    sourceField: ['prescriberId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const labOrderRelationships = relationships(labOrder, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  orderingProvider: one({
    sourceField: ['orderingProviderId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const labResultRelationships = relationships(labResult, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  order: one({
    sourceField: ['orderId'],
    destField: ['orderId'],
    destSchema: labOrder,
  }),
  reviewedByUser: one({
    sourceField: ['reviewedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const insuranceClaimRelationships = relationships(insuranceClaim, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
}));

const diagnosisRelationships = relationships(diagnosis, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  provider: one({
    sourceField: ['providerId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const vitalSignsRelationships = relationships(vitalSigns, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  recordedByUser: one({
    sourceField: ['recordedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const immunizationRelationships = relationships(immunization, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  patient: one({
    sourceField: ['patientId'],
    destField: ['patientId'],
    destSchema: patient,
  }),
  provider: one({
    sourceField: ['providerId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== REAL ESTATE PLATFORM ====================

const propertyRelationships = relationships(property, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  agent: one({
    sourceField: ['agentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const listingRelationships = relationships(listing, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  property: one({
    sourceField: ['propertyId'],
    destField: ['propertyId'],
    destSchema: property,
  }),
  listingAgent: one({
    sourceField: ['listingAgentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  coListingAgent: one({
    sourceField: ['coListingAgentId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const showingRelationships = relationships(showing, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  listing: one({
    sourceField: ['listingId'],
    destField: ['listingId'],
    destSchema: listing,
  }),
  buyerAgent: one({
    sourceField: ['buyerAgentId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const offerRelationships = relationships(offer, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  listing: one({
    sourceField: ['listingId'],
    destField: ['listingId'],
    destSchema: listing,
  }),
  buyerAgent: one({
    sourceField: ['buyerAgentId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const contractRelationships = relationships(contract, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  offer: one({
    sourceField: ['offerId'],
    destField: ['offerId'],
    destSchema: offer,
  }),
  listing: one({
    sourceField: ['listingId'],
    destField: ['listingId'],
    destSchema: listing,
  }),
  buyerAgent: one({
    sourceField: ['buyerAgentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  sellerAgent: one({
    sourceField: ['sellerAgentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const propertyDocumentRelationships = relationships(
  propertyDocument,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    property: one({
      sourceField: ['propertyId'],
      destField: ['propertyId'],
      destSchema: property,
    }),
    uploadedByUser: one({
      sourceField: ['uploadedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const buyerProfileRelationships = relationships(buyerProfile, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  agent: one({
    sourceField: ['agentId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const marketAnalysisRelationships = relationships(marketAnalysis, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  property: one({
    sourceField: ['propertyId'],
    destField: ['propertyId'],
    destSchema: property,
  }),
  analyzedByUser: one({
    sourceField: ['analyzedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const inspectionReportRelationships = relationships(
  inspectionReport,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    property: one({
      sourceField: ['propertyId'],
      destField: ['propertyId'],
      destSchema: property,
    }),
  }),
);

const commissionSplitRelationships = relationships(
  commissionSplit,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    contract: one({
      sourceField: ['contractId'],
      destField: ['contractId'],
      destSchema: contract,
    }),
    agent: one({
      sourceField: ['agentId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

// ==================== LEARNING MANAGEMENT SYSTEM ====================

const courseRelationships = relationships(course, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
  updatedByUser: one({
    sourceField: ['updatedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const courseModuleRelationships = relationships(courseModule, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  course: one({
    sourceField: ['courseId'],
    destField: ['courseId'],
    destSchema: course,
  }),
}));

const lessonRelationships = relationships(lesson, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  module: one({
    sourceField: ['moduleId'],
    destField: ['moduleId'],
    destSchema: courseModule,
  }),
}));

const enrollmentRelationships = relationships(enrollment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  course: one({
    sourceField: ['courseId'],
    destField: ['courseId'],
    destSchema: course,
  }),
  student: one({
    sourceField: ['studentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  currentModule: one({
    sourceField: ['currentModuleId'],
    destField: ['moduleId'],
    destSchema: courseModule,
  }),
}));

const assignmentRelationships = relationships(assignment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  course: one({
    sourceField: ['courseId'],
    destField: ['courseId'],
    destSchema: course,
  }),
  module: one({
    sourceField: ['moduleId'],
    destField: ['moduleId'],
    destSchema: courseModule,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const submissionRelationships = relationships(submission, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  assignment: one({
    sourceField: ['assignmentId'],
    destField: ['assignmentId'],
    destSchema: assignment,
  }),
  student: one({
    sourceField: ['studentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  gradedByUser: one({
    sourceField: ['gradedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const quizRelationships = relationships(quiz, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  course: one({
    sourceField: ['courseId'],
    destField: ['courseId'],
    destSchema: course,
  }),
  module: one({
    sourceField: ['moduleId'],
    destField: ['moduleId'],
    destSchema: courseModule,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const quizAttemptRelationships = relationships(quizAttempt, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  quiz: one({
    sourceField: ['quizId'],
    destField: ['quizId'],
    destSchema: quiz,
  }),
  student: one({
    sourceField: ['studentId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const discussionThreadRelationships = relationships(
  discussionThread,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    course: one({
      sourceField: ['courseId'],
      destField: ['courseId'],
      destSchema: course,
    }),
    author: one({
      sourceField: ['authorId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const certificateRelationships = relationships(certificate, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  enrollment: one({
    sourceField: ['enrollmentId'],
    destField: ['enrollmentId'],
    destSchema: enrollment,
  }),
  student: one({
    sourceField: ['studentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  course: one({
    sourceField: ['courseId'],
    destField: ['courseId'],
    destSchema: course,
  }),
}));

// ==================== VIDEO STREAMING PLATFORM ====================

const videoRelationships = relationships(video, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  channel: one({
    sourceField: ['channelId'],
    destField: ['channelId'],
    destSchema: channel,
  }),
  uploadedByUser: one({
    sourceField: ['uploadedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const videoMetadataRelationships = relationships(videoMetadata, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  video: one({
    sourceField: ['videoId'],
    destField: ['videoId'],
    destSchema: video,
  }),
}));

const playlistRelationships = relationships(playlist, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const subscriptionRelationships = relationships(subscription, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  subscriberUser: one({
    sourceField: ['subscriberUserId'],
    destField: ['userId'],
    destSchema: user,
  }),
  channel: one({
    sourceField: ['channelId'],
    destField: ['channelId'],
    destSchema: channel,
  }),
}));

const watchHistoryRelationships = relationships(watchHistory, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
  video: one({
    sourceField: ['videoId'],
    destField: ['videoId'],
    destSchema: video,
  }),
}));

const videoCommentRelationships = relationships(videoComment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  video: one({
    sourceField: ['videoId'],
    destField: ['videoId'],
    destSchema: video,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['userId'],
    destSchema: user,
  }),
  parentComment: one({
    sourceField: ['parentCommentId'],
    destField: ['commentId'],
    destSchema: videoComment,
  }),
}));

const channelRelationships = relationships(channel, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  owner: one({
    sourceField: ['ownerId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const videoAnalyticsRelationships = relationships(videoAnalytics, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  video: one({
    sourceField: ['videoId'],
    destField: ['videoId'],
    destSchema: video,
  }),
}));

const livestreamRelationships = relationships(livestream, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  channel: one({
    sourceField: ['channelId'],
    destField: ['channelId'],
    destSchema: channel,
  }),
}));

const contentRestrictionRelationships = relationships(
  contentRestriction,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    video: one({
      sourceField: ['videoId'],
      destField: ['videoId'],
      destSchema: video,
    }),
  }),
);

// ==================== SOCIAL MEDIA PLATFORM ====================

const profileRelationships = relationships(profile, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({sourceField: ['userId'], destField: ['userId'], destSchema: user}),
}));

const postRelationships = relationships(post, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['userId'],
    destSchema: user,
  }),
  repostOfPost: one({
    sourceField: ['repostOf'],
    destField: ['postId'],
    destSchema: post,
  }),
}));

const postMediaRelationships = relationships(postMedia, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  post: one({sourceField: ['postId'], destField: ['postId'], destSchema: post}),
}));

const followRelationshipRelationships = relationships(
  followRelationship,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    follower: one({
      sourceField: ['followerId'],
      destField: ['userId'],
      destSchema: user,
    }),
    following: one({
      sourceField: ['followingId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const likeRelationships = relationships(like, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({sourceField: ['userId'], destField: ['userId'], destSchema: user}),
}));

const commentRelationships = relationships(comment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  post: one({sourceField: ['postId'], destField: ['postId'], destSchema: post}),
  author: one({
    sourceField: ['authorId'],
    destField: ['userId'],
    destSchema: user,
  }),
  parentComment: one({
    sourceField: ['parentCommentId'],
    destField: ['commentId'],
    destSchema: comment,
  }),
}));

const directMessageRelationships = relationships(directMessage, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  conversation: one({
    sourceField: ['conversationId'],
    destField: ['conversationId'],
    destSchema: dmConversation,
  }),
  sender: one({
    sourceField: ['senderId'],
    destField: ['userId'],
    destSchema: user,
  }),
  replyToMessage: one({
    sourceField: ['replyToMessageId'],
    destField: ['messageId'],
    destSchema: directMessage,
  }),
}));

const dmConversationRelationships = relationships(dmConversation, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  lastMessage: one({
    sourceField: ['lastMessageId'],
    destField: ['messageId'],
    destSchema: directMessage,
  }),
}));

const notificationRelationships = relationships(notification, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  user: one({sourceField: ['userId'], destField: ['userId'], destSchema: user}),
  actor: one({
    sourceField: ['actorId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const blockRelationshipRelationships = relationships(
  blockRelationship,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    blockerUser: one({
      sourceField: ['blockerUserId'],
      destField: ['userId'],
      destSchema: user,
    }),
    blockedUser: one({
      sourceField: ['blockedUserId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

// ==================== LEGAL PRACTICE MANAGEMENT ====================

const legalCaseRelationships = relationships(legalCase, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  leadAttorney: one({
    sourceField: ['leadAttorneyId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const casePartyRelationships = relationships(caseParty, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  case: one({
    sourceField: ['caseId'],
    destField: ['caseId'],
    destSchema: legalCase,
  }),
  attorney: one({
    sourceField: ['attorneyId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const legalDocumentRelationships = relationships(legalDocument, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  case: one({
    sourceField: ['caseId'],
    destField: ['caseId'],
    destSchema: legalCase,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const courtDateRelationships = relationships(courtDate, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  case: one({
    sourceField: ['caseId'],
    destField: ['caseId'],
    destSchema: legalCase,
  }),
}));

const billableTimeRelationships = relationships(billableTime, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  case: one({
    sourceField: ['caseId'],
    destField: ['caseId'],
    destSchema: legalCase,
  }),
  attorney: one({
    sourceField: ['attorneyId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const matterRelationships = relationships(matter, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  responsibleAttorney: one({
    sourceField: ['responsibleAttorneyId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const trustTransactionRelationships = relationships(
  trustTransaction,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    matter: one({
      sourceField: ['matterId'],
      destField: ['matterId'],
      destSchema: matter,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const conflictCheckRelationships = relationships(conflictCheck, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  checkedByUser: one({
    sourceField: ['checkedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const retainerAgreementRelationships = relationships(
  retainerAgreement,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    matter: one({
      sourceField: ['matterId'],
      destField: ['matterId'],
      destSchema: matter,
    }),
  }),
);

const legalResearchRelationships = relationships(legalResearch, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  matter: one({
    sourceField: ['matterId'],
    destField: ['matterId'],
    destSchema: matter,
  }),
  attorney: one({
    sourceField: ['attorneyId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== RESTAURANT/FOOD SERVICE ====================

const menuRelationships = relationships(menu, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const menuItemRelationships = relationships(menuItem, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  menu: one({sourceField: ['menuId'], destField: ['menuId'], destSchema: menu}),
}));

const ingredientRelationships = relationships(ingredient, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  supplier: one({
    sourceField: ['supplierId'],
    destField: ['supplierId'],
    destSchema: supplier,
  }),
}));

const recipeRelationships = relationships(recipe, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  menuItem: one({
    sourceField: ['menuItemId'],
    destField: ['itemId'],
    destSchema: menuItem,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const restaurantOrderRelationships = relationships(
  restaurantOrder,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    server: one({
      sourceField: ['serverId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const restaurantOrderItemRelationships = relationships(
  restaurantOrderItem,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    order: one({
      sourceField: ['restaurantOrderId'],
      destField: ['restaurantOrderId'],
      destSchema: restaurantOrder,
    }),
    menuItem: one({
      sourceField: ['menuItemId'],
      destField: ['itemId'],
      destSchema: menuItem,
    }),
  }),
);

const restaurantReservationRelationships = relationships(
  restaurantReservation,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
  }),
);

const supplierRelationships = relationships(supplier, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const purchaseOrderRelationships = relationships(purchaseOrder, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  supplier: one({
    sourceField: ['supplierId'],
    destField: ['supplierId'],
    destSchema: supplier,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const inventoryCountRelationships = relationships(inventoryCount, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  ingredient: one({
    sourceField: ['ingredientId'],
    destField: ['ingredientId'],
    destSchema: ingredient,
  }),
  countedByUser: one({
    sourceField: ['countedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

// ==================== LOGISTICS/SHIPPING ====================

const shipmentRelationships = relationships(shipment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const shipmentItemRelationships = relationships(shipmentItem, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  shipment: one({
    sourceField: ['shipmentId'],
    destField: ['shipmentId'],
    destSchema: shipment,
  }),
}));

const routeRelationships = relationships(route, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  driver: one({
    sourceField: ['driverId'],
    destField: ['driverId'],
    destSchema: driver,
  }),
  vehicle: one({
    sourceField: ['vehicleId'],
    destField: ['vehicleId'],
    destSchema: vehicle,
  }),
}));

const trackingEventRelationships = relationships(trackingEvent, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  shipment: one({
    sourceField: ['shipmentId'],
    destField: ['shipmentId'],
    destSchema: shipment,
  }),
}));

const driverRelationships = relationships(driver, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const vehicleRelationships = relationships(vehicle, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const warehouseRelationships = relationships(warehouse, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  manager: one({
    sourceField: ['managerId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const warehouseZoneRelationships = relationships(warehouseZone, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  warehouse: one({
    sourceField: ['warehouseId'],
    destField: ['warehouseId'],
    destSchema: warehouse,
  }),
}));

const deliveryManifestRelationships = relationships(
  deliveryManifest,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    route: one({
      sourceField: ['routeId'],
      destField: ['routeId'],
      destSchema: route,
    }),
  }),
);

const freightInvoiceRelationships = relationships(freightInvoice, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  shipment: one({
    sourceField: ['shipmentId'],
    destField: ['shipmentId'],
    destSchema: shipment,
  }),
}));

// ==================== INSURANCE PLATFORM ====================

const policyRelationships = relationships(policy, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  agent: one({
    sourceField: ['agentId'],
    destField: ['userId'],
    destSchema: user,
  }),
  underwriter: one({
    sourceField: ['underwriterId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const policyCoverageRelationships = relationships(policyCoverage, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  policy: one({
    sourceField: ['policyId'],
    destField: ['policyId'],
    destSchema: policy,
  }),
}));

const claimRelationships = relationships(claim, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  policy: one({
    sourceField: ['policyId'],
    destField: ['policyId'],
    destSchema: policy,
  }),
  adjuster: one({
    sourceField: ['adjusterId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const claimPaymentRelationships = relationships(claimPayment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  claim: one({
    sourceField: ['claimId'],
    destField: ['claimId'],
    destSchema: claim,
  }),
}));

const underwritingReviewRelationships = relationships(
  underwritingReview,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    reviewer: one({
      sourceField: ['reviewerId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const policyDocumentRelationships = relationships(policyDocument, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  policy: one({
    sourceField: ['policyId'],
    destField: ['policyId'],
    destSchema: policy,
  }),
}));

const premiumPaymentRelationships = relationships(premiumPayment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  policy: one({
    sourceField: ['policyId'],
    destField: ['policyId'],
    destSchema: policy,
  }),
}));

const adjusterAssignmentRelationships = relationships(
  adjusterAssignment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    claim: one({
      sourceField: ['claimId'],
      destField: ['claimId'],
      destSchema: claim,
    }),
    adjuster: one({
      sourceField: ['adjusterId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const lossHistoryRelationships = relationships(lossHistory, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  claim: one({
    sourceField: ['claimId'],
    destField: ['claimId'],
    destSchema: claim,
  }),
}));

const reinsuranceTreatyRelationships = relationships(
  reinsuranceTreaty,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
  }),
);

// ==================== HOTEL/HOSPITALITY ====================

const roomRelationships = relationships(room, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const reservationRelationships = relationships(reservation, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  guest: one({
    sourceField: ['guestId'],
    destField: ['guestId'],
    destSchema: guest,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const guestRelationships = relationships(guest, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const ratePlanRelationships = relationships(ratePlan, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const housekeepingTaskRelationships = relationships(
  housekeepingTask,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    room: one({
      sourceField: ['roomId'],
      destField: ['roomId'],
      destSchema: room,
    }),
    assignedToUser: one({
      sourceField: ['assignedTo'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const guestServiceRequestRelationships = relationships(
  guestServiceRequest,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    reservation: one({
      sourceField: ['reservationId'],
      destField: ['reservationId'],
      destSchema: reservation,
    }),
    guest: one({
      sourceField: ['guestId'],
      destField: ['guestId'],
      destSchema: guest,
    }),
    assignedToUser: one({
      sourceField: ['assignedTo'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const amenityBookingRelationships = relationships(amenityBooking, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  reservation: one({
    sourceField: ['reservationId'],
    destField: ['reservationId'],
    destSchema: reservation,
  }),
  guest: one({
    sourceField: ['guestId'],
    destField: ['guestId'],
    destSchema: guest,
  }),
}));

const roomRateOverrideRelationships = relationships(
  roomRateOverride,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    reservation: one({
      sourceField: ['reservationId'],
      destField: ['reservationId'],
      destSchema: reservation,
    }),
    approvedByUser: one({
      sourceField: ['approvedById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const folioChargeRelationships = relationships(folioCharge, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  reservation: one({
    sourceField: ['reservationId'],
    destField: ['reservationId'],
    destSchema: reservation,
  }),
  postedByUser: one({
    sourceField: ['postedById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const maintenanceIssueRelationships = relationships(
  maintenanceIssue,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    room: one({
      sourceField: ['roomId'],
      destField: ['roomId'],
      destSchema: room,
    }),
    reportedByUser: one({
      sourceField: ['reportedById'],
      destField: ['userId'],
      destSchema: user,
    }),
    assignedToUser: one({
      sourceField: ['assignedTo'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

// ==================== MARKETING AUTOMATION ====================

const marketingCampaignRelationships = relationships(
  marketingCampaign,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const leadRelationships = relationships(lead, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  assignedToUser: one({
    sourceField: ['assignedTo'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const segmentRelationships = relationships(segment, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const customerJourneyRelationships = relationships(
  customerJourney,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    createdByUser: one({
      sourceField: ['createdById'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const abTestRelationships = relationships(abTest, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const leadActivityRelationships = relationships(leadActivity, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  lead: one({sourceField: ['leadId'], destField: ['leadId'], destSchema: lead}),
}));

const attributionTouchpointRelationships = relationships(
  attributionTouchpoint,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    lead: one({
      sourceField: ['leadId'],
      destField: ['leadId'],
      destSchema: lead,
    }),
    campaign: one({
      sourceField: ['campaignId'],
      destField: ['campaignId'],
      destSchema: marketingCampaign,
    }),
  }),
);

const conversionGoalRelationships = relationships(conversionGoal, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const marketingAssetRelationships = relationships(marketingAsset, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const formSubmissionRelationships = relationships(formSubmission, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  lead: one({sourceField: ['leadId'], destField: ['leadId'], destSchema: lead}),
}));

// ==================== RECRUITMENT/ATS ====================

const jobPostingRelationships = relationships(jobPosting, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  hiringManager: one({
    sourceField: ['hiringManagerId'],
    destField: ['userId'],
    destSchema: user,
  }),
  recruiter: one({
    sourceField: ['recruiterId'],
    destField: ['userId'],
    destSchema: user,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const candidateRelationships = relationships(candidate, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const applicationRelationships = relationships(application, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const interviewRelationships = relationships(interview, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const interviewFeedbackRelationships = relationships(
  interviewFeedback,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    interviewer: one({
      sourceField: ['interviewerId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

const jobOfferRelationships = relationships(jobOffer, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const candidateNoteRelationships = relationships(candidateNote, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const hiringPipelineRelationships = relationships(hiringPipeline, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const onboardingTaskRelationships = relationships(onboardingTask, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  assignee: one({
    sourceField: ['assigneeId'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const recruiterAssignmentRelationships = relationships(
  recruiterAssignment,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    recruiter: one({
      sourceField: ['recruiterId'],
      destField: ['userId'],
      destSchema: user,
    }),
  }),
);

// ==================== EVENT MANAGEMENT ====================

const eventRelationships = relationships(event, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  venue: one({
    sourceField: ['venueId'],
    destField: ['venueId'],
    destSchema: venue,
  }),
  createdByUser: one({
    sourceField: ['createdById'],
    destField: ['userId'],
    destSchema: user,
  }),
}));

const venueRelationships = relationships(venue, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const ticketTypeRelationships = relationships(ticketType, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  event: one({
    sourceField: ['eventId'],
    destField: ['eventId'],
    destSchema: event,
  }),
}));

const ticketPurchaseRelationships = relationships(ticketPurchase, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  event: one({
    sourceField: ['eventId'],
    destField: ['eventId'],
    destSchema: event,
  }),
}));

const attendeeRelationships = relationships(attendee, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  ticketPurchase: one({
    sourceField: ['ticketPurchaseId'],
    destField: ['purchaseId'],
    destSchema: ticketPurchase,
  }),
  event: one({
    sourceField: ['eventId'],
    destField: ['eventId'],
    destSchema: event,
  }),
}));

const eventSessionRelationships = relationships(eventSession, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  event: one({
    sourceField: ['eventId'],
    destField: ['eventId'],
    destSchema: event,
  }),
}));

const speakerRelationships = relationships(speaker, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
}));

const sponsorRelationships = relationships(sponsor, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  event: one({
    sourceField: ['eventId'],
    destField: ['eventId'],
    destSchema: event,
  }),
}));

const sessionRegistrationRelationships = relationships(
  sessionRegistration,
  ({one}) => ({
    workspace: one({
      sourceField: ['workspaceId'],
      destField: ['workspaceId'],
      destSchema: workspace,
    }),
    session: one({
      sourceField: ['sessionId'],
      destField: ['sessionId'],
      destSchema: eventSession,
    }),
    attendee: one({
      sourceField: ['attendeeId'],
      destField: ['attendeeId'],
      destSchema: attendee,
    }),
  }),
);

const eventScheduleRelationships = relationships(eventSchedule, ({one}) => ({
  workspace: one({
    sourceField: ['workspaceId'],
    destField: ['workspaceId'],
    destSchema: workspace,
  }),
  event: one({
    sourceField: ['eventId'],
    destField: ['eventId'],
    destSchema: event,
  }),
}));

export const zeroStressSchema = createSchema({
  tables: [
    abTest,
    account,
    adjusterAssignment,
    agentAssignment,
    amenityBooking,
    apiKey,
    application,
    appointment,
    assignment,
    attendanceLog,
    attendee,
    attributionTouchpoint,
    auditLog,
    automationWorkflow,
    bankTransaction,
    benefitsEnrollment,
    billableTime,
    blockRelationship,
    board,
    budget,
    buyerProfile,
    candidate,
    candidateNote,
    cannedResponse,
    caseParty,
    certificate,
    channel,
    claim,
    claimPayment,
    cmsArticle,
    cmsComment,
    cmsMenu,
    cmsPage,
    comment,
    commissionSplit,
    compensationChange,
    conflictCheck,
    contentBlock,
    contentLock,
    contentRestriction,
    contentRevision,
    contract,
    conversionGoal,
    course,
    courseModule,
    courtDate,
    customerJourney,
    customFieldDefinition,
    customFieldValue,
    deliveryManifest,
    department,
    diagnosis,
    directMessage,
    discountCode,
    discussionThread,
    dmConversation,
    driver,
    emailAttachment,
    emailCampaign,
    emailLink,
    emailSend,
    emailTemplate,
    employee,
    enrollment,
    entityAttachment,
    entityComment,
    entityTag,
    event,
    eventSchedule,
    eventSession,
    expenseClaim,
    featureFlag,
    folioCharge,
    followRelationship,
    formSubmission,
    freightInvoice,
    guest,
    guestServiceRequest,
    hiringPipeline,
    housekeepingTask,
    immunization,
    ingredient,
    inspectionReport,
    insuranceClaim,
    integration,
    interview,
    interviewFeedback,
    inventoryAdjustment,
    inventoryCount,
    invoice,
    jobOffer,
    jobPosting,
    journalEntry,
    journalLine,
    knowledgeBaseArticle,
    labOrder,
    labResult,
    lead,
    leadActivity,
    ledgerAccount,
    legalCase,
    legalDocument,
    legalResearch,
    lesson,
    like,
    listing,
    livestream,
    lossHistory,
    maintenanceIssue,
    marketAnalysis,
    marketingAsset,
    marketingCampaign,
    matter,
    mediaAsset,
    medicalRecord,
    menu,
    menuItem,
    milestone,
    notification,
    offer,
    onboardingTask,
    order,
    orderLineItem,
    passwordReset,
    patient,
    payment,
    paymentTransaction,
    payrollLine,
    payrollRun,
    performanceReview,
    playlist,
    policy,
    policyCoverage,
    policyDocument,
    post,
    postMedia,
    premiumPayment,
    prescription,
    product,
    productReview,
    productVariant,
    profile,
    project,
    projectBudget,
    property,
    propertyDocument,
    purchaseOrder,
    quiz,
    quizAttempt,
    rateLimit,
    ratePlan,
    recipe,
    reconciliation,
    recruiterAssignment,
    redirectRule,
    reinsuranceTreaty,
    reservation,
    resourceAllocation,
    restaurantOrder,
    restaurantOrderItem,
    restaurantReservation,
    retainerAgreement,
    room,
    roomRateOverride,
    route,
    satisfactionSurvey,
    segment,
    session,
    sessionRegistration,
    shipment,
    shipmentItem,
    shippingZone,
    shoppingCart,
    showing,
    slaPolicy,
    spamComplaint,
    speaker,
    sponsor,
    sprint,
    submission,
    subscriber,
    subscriberList,
    subscription,
    supplier,
    supportTicket,
    task,
    taskComment,
    taskDependency,
    taxonomyTerm,
    taxRate,
    team,
    ticketEscalation,
    ticketMerge,
    ticketMessage,
    ticketPurchase,
    ticketTag,
    ticketType,
    timeEntry,
    timeOffRequest,
    trackingEvent,
    trainingRecord,
    trustTransaction,
    underwritingReview,
    unsubscribeEvent,
    user,
    vehicle,
    venue,
    verificationToken,
    video,
    videoAnalytics,
    videoComment,
    videoMetadata,
    vitalSigns,
    warehouse,
    warehouseZone,
    watchHistory,
    webhook,
    webhookDelivery,
    workspace,
    workspaceMember,
  ],
  relationships: [
    abTestRelationships,
    accountRelationships,
    adjusterAssignmentRelationships,
    agentAssignmentRelationships,
    amenityBookingRelationships,
    apiKeyRelationships,
    applicationRelationships,
    appointmentRelationships,
    assignmentRelationships,
    attendanceLogRelationships,
    attendeeRelationships,
    attributionTouchpointRelationships,
    auditLogRelationships,
    automationWorkflowRelationships,
    bankTransactionRelationships,
    benefitsEnrollmentRelationships,
    billableTimeRelationships,
    blockRelationshipRelationships,
    boardRelationships,
    budgetRelationships,
    buyerProfileRelationships,
    candidateNoteRelationships,
    candidateRelationships,
    cannedResponseRelationships,
    casePartyRelationships,
    certificateRelationships,
    channelRelationships,
    claimPaymentRelationships,
    claimRelationships,
    cmsArticleRelationships,
    cmsCommentRelationships,
    cmsMenuRelationships,
    cmsPageRelationships,
    commentRelationships,
    commissionSplitRelationships,
    compensationChangeRelationships,
    conflictCheckRelationships,
    contentBlockRelationships,
    contentLockRelationships,
    contentRestrictionRelationships,
    contentRevisionRelationships,
    contractRelationships,
    conversionGoalRelationships,
    courseModuleRelationships,
    courseRelationships,
    courtDateRelationships,
    customerJourneyRelationships,
    customFieldDefinitionRelationships,
    customFieldValueRelationships,
    deliveryManifestRelationships,
    departmentRelationships,
    diagnosisRelationships,
    directMessageRelationships,
    discountCodeRelationships,
    discussionThreadRelationships,
    dmConversationRelationships,
    driverRelationships,
    emailAttachmentRelationships,
    emailCampaignRelationships,
    emailLinkRelationships,
    emailSendRelationships,
    emailTemplateRelationships,
    employeeRelationships,
    enrollmentRelationships,
    entityAttachmentRelationships,
    entityCommentRelationships,
    entityTagRelationships,
    eventRelationships,
    eventScheduleRelationships,
    eventSessionRelationships,
    expenseClaimRelationships,
    featureFlagRelationships,
    folioChargeRelationships,
    followRelationshipRelationships,
    formSubmissionRelationships,
    freightInvoiceRelationships,
    guestRelationships,
    guestServiceRequestRelationships,
    hiringPipelineRelationships,
    housekeepingTaskRelationships,
    immunizationRelationships,
    ingredientRelationships,
    inspectionReportRelationships,
    insuranceClaimRelationships,
    integrationRelationships,
    interviewFeedbackRelationships,
    interviewRelationships,
    inventoryAdjustmentRelationships,
    inventoryCountRelationships,
    invoiceRelationships,
    jobOfferRelationships,
    jobPostingRelationships,
    journalEntryRelationships,
    journalLineRelationships,
    knowledgeBaseArticleRelationships,
    labOrderRelationships,
    labResultRelationships,
    leadActivityRelationships,
    leadRelationships,
    ledgerAccountRelationships,
    legalCaseRelationships,
    legalDocumentRelationships,
    legalResearchRelationships,
    lessonRelationships,
    likeRelationships,
    listingRelationships,
    livestreamRelationships,
    lossHistoryRelationships,
    maintenanceIssueRelationships,
    marketAnalysisRelationships,
    marketingAssetRelationships,
    marketingCampaignRelationships,
    matterRelationships,
    mediaAssetRelationships,
    medicalRecordRelationships,
    menuItemRelationships,
    menuRelationships,
    milestoneRelationships,
    notificationRelationships,
    offerRelationships,
    onboardingTaskRelationships,
    orderLineItemRelationships,
    orderRelationships,
    passwordResetRelationships,
    patientRelationships,
    paymentRelationships,
    paymentTransactionRelationships,
    payrollLineRelationships,
    payrollRunRelationships,
    performanceReviewRelationships,
    playlistRelationships,
    policyCoverageRelationships,
    policyDocumentRelationships,
    policyRelationships,
    postMediaRelationships,
    postRelationships,
    premiumPaymentRelationships,
    prescriptionRelationships,
    productRelationships,
    productReviewRelationships,
    productVariantRelationships,
    profileRelationships,
    projectBudgetRelationships,
    projectRelationships,
    propertyDocumentRelationships,
    propertyRelationships,
    purchaseOrderRelationships,
    quizAttemptRelationships,
    quizRelationships,
    rateLimitRelationships,
    ratePlanRelationships,
    recipeRelationships,
    reconciliationRelationships,
    recruiterAssignmentRelationships,
    redirectRuleRelationships,
    reinsuranceTreatyRelationships,
    reservationRelationships,
    resourceAllocationRelationships,
    restaurantOrderItemRelationships,
    restaurantOrderRelationships,
    restaurantReservationRelationships,
    retainerAgreementRelationships,
    roomRateOverrideRelationships,
    roomRelationships,
    routeRelationships,
    satisfactionSurveyRelationships,
    segmentRelationships,
    sessionRegistrationRelationships,
    sessionRelationships,
    shipmentItemRelationships,
    shipmentRelationships,
    shippingZoneRelationships,
    shoppingCartRelationships,
    showingRelationships,
    slaPolicyRelationships,
    spamComplaintRelationships,
    speakerRelationships,
    sponsorRelationships,
    sprintRelationships,
    submissionRelationships,
    subscriberListRelationships,
    subscriberRelationships,
    subscriptionRelationships,
    supplierRelationships,
    supportTicketRelationships,
    taskCommentRelationships,
    taskDependencyRelationships,
    taskRelationships,
    taxonomyTermRelationships,
    taxRateRelationships,
    teamRelationships,
    ticketEscalationRelationships,
    ticketMergeRelationships,
    ticketMessageRelationships,
    ticketPurchaseRelationships,
    ticketTagRelationships,
    ticketTypeRelationships,
    timeEntryRelationships,
    timeOffRequestRelationships,
    trackingEventRelationships,
    trainingRecordRelationships,
    trustTransactionRelationships,
    underwritingReviewRelationships,
    unsubscribeEventRelationships,
    userRelationships,
    vehicleRelationships,
    venueRelationships,
    verificationTokenRelationships,
    videoAnalyticsRelationships,
    videoCommentRelationships,
    videoMetadataRelationships,
    videoRelationships,
    vitalSignsRelationships,
    warehouseRelationships,
    warehouseZoneRelationships,
    watchHistoryRelationships,
    webhookDeliveryRelationships,
    webhookRelationships,
    workspaceRelationships,
    workspaceMemberRelationships,
  ],
  enableLegacyMutators: true,
  enableLegacyQueries: false,
});
