// This file outputs the queries:
// deep: 20 levels of deeply nested .related() calls
// wide: 90 distinct .related() calls on workspace table
// then 22 simple queries with two .related() calls
// this is the maximum that can be serialized by the compiler

import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {
  defineQueries,
  defineQuery,
} from '../../../zql/src/query/query-registry.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {zeroStressSchema} from './zero-stress-schema-test.ts';
import type {StressContext} from './zero-stress-shared-test.ts';

const zql = createBuilder(zeroStressSchema);

const defineQueryTyped = <
  TTable extends keyof (typeof zeroStressSchema)['tables'],
  TArgs extends ReadonlyJSONValue | undefined,
  TReturn,
>(
  query: (options: {
    args: TArgs;
    ctx: StressContext;
  }) => Query<TTable, typeof zeroStressSchema, TReturn>,
) =>
  defineQuery<
    TTable,
    typeof zeroStressSchema,
    TReturn,
    StressContext,
    TArgs,
    TArgs
  >(((v: unknown) => v) as unknown as StandardSchemaV1<TArgs, TArgs>, query);

const queryWide = defineQueryTyped(() =>
  zql.workspace
    .related('sessions')
    .related('accounts')
    .related('workspaceMembers')
    .related('teams')
    .related('apiKeys')
    .related('verificationTokens')
    .related('passwordResets')
    .related('auditLogs')
    .related('entityTags')
    .related('entityAttachments')
    .related('entityComments')
    .related('customFieldDefinitions')
    .related('customFieldValues')
    .related('webhooks')
    .related('webhookDeliveries')
    .related('rateLimits')
    .related('featureFlags')
    .related('integrations')
    .related('emailCampaigns')
    .related('emailTemplates')
    .related('emailSends')
    .related('subscriberLists')
    .related('subscribers')
    .related('automationWorkflows')
    .related('emailLinks')
    .related('unsubscribeEvents')
    .related('emailAttachments')
    .related('spamComplaints')
    .related('ticketMessages')
    .related('knowledgeBaseArticles')
    .related('slaPolicies')
    .related('cannedResponses')
    .related('supportTickets')
    .related('ticketTags')
    .related('satisfactionSurveys')
    .related('agentAssignments')
    .related('ticketEscalations')
    .related('ticketMerges')
    .related('products')
    .related('productVariants')
    .related('orders')
    .related('orderLineItems')
    .related('shoppingCarts')
    .related('productReviews')
    .related('inventoryAdjustments')
    .related('discountCodes')
    .related('shippingZones')
    .related('paymentTransactions')
    .related('cmsArticles')
    .related('cmsPages')
    .related('mediaAssets')
    .related('contentRevisions')
    .related('taxonomyTerms')
    .related('contentBlocks')
    .related('cmsMenus')
    .related('redirectRules')
    .related('cmsComments')
    .related('contentLocks')
    .related('projects')
    .related('tasks')
    .related('taskDependencies')
    .related('sprints')
    .related('boards')
    .related('timeEntries')
    .related('taskComments')
    .related('milestones')
    .related('projectBudgets')
    .related('resourceAllocations')
    .related('employees')
    .related('payrollRuns')
    .related('payrollLines')
    .related('timeOffRequests')
    .related('benefitsEnrollments')
    .related('performanceReviews')
    .related('departments')
    .related('compensationChanges')
    .related('trainingRecords')
    .related('attendanceLogs')
    .related('ledgerAccounts')
    .related('journalEntries')
    .related('journalLines')
    .related('invoices')
    .related('payments')
    .related('bankTransactions')
    .related('expenseClaims')
    .related('budgets')
    .related('taxRates')
    .related('reconciliations')
    .related('patients')
    .related('appointments'),
);

const queryDeep = defineQueryTyped(() =>
  zql.order.related('createdByUser', q =>
    q.related('workspaceMembers', q =>
      q.related('workspace', q =>
        q.related('budgets', q =>
          q.related('department', q =>
            q.related('parentDepartment', q =>
              q.related('headOfDepartment', q =>
                q.related('manager', q =>
                  q.related('workspace', q =>
                    q.related('agentAssignments', q =>
                      q.related('ticket', q =>
                        q.related('team', q =>
                          q.related('leader', q =>
                            q.related('updatedCmsArticles', q =>
                              q.related('author', q =>
                                q.related('ownedProjects', q =>
                                  q.related('owner', q =>
                                    q.related('updatedEntityComments', q =>
                                      q.related('parentComment', q =>
                                        q.related('createdByUser'),
                                      ),
                                    ),
                                  ),
                                ),
                              ),
                            ),
                          ),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    ),
  ),
);

// 100 simple queries with consistent pattern: related('workspace', q => q.related('teams'))
const q01 = defineQueryTyped(() =>
  zql.user.related('sessions').related('accounts'),
);
const q02 = defineQueryTyped(() =>
  zql.session.related('workspace', q => q.related('teams')),
);
const q03 = defineQueryTyped(() =>
  zql.account.related('workspace', q => q.related('teams')),
);
const q04 = defineQueryTyped(() =>
  zql.workspace.related('workspaceMembers', q => q.related('workspace')),
);
const q05 = defineQueryTyped(() =>
  zql.workspaceMember.related('workspace', q => q.related('teams')),
);
const q06 = defineQueryTyped(() =>
  zql.team.related('workspace', q => q.related('teams')),
);
const q07 = defineQueryTyped(() =>
  zql.apiKey.related('workspace', q => q.related('teams')),
);
const q08 = defineQueryTyped(() =>
  zql.verificationToken.related('workspace', q => q.related('teams')),
);
const q09 = defineQueryTyped(() =>
  zql.passwordReset.related('workspace', q => q.related('teams')),
);
const q10 = defineQueryTyped(() =>
  zql.auditLog.related('workspace', q => q.related('teams')),
);
const q11 = defineQueryTyped(() =>
  zql.entityTag.related('workspace', q => q.related('teams')),
);
const q12 = defineQueryTyped(() =>
  zql.entityAttachment.related('workspace', q => q.related('teams')),
);
const q13 = defineQueryTyped(() =>
  zql.entityComment.related('workspace', q => q.related('teams')),
);
const q14 = defineQueryTyped(() =>
  zql.customFieldDefinition.related('workspace', q => q.related('teams')),
);
const q15 = defineQueryTyped(() =>
  zql.customFieldValue.related('workspace', q => q.related('teams')),
);
const q16 = defineQueryTyped(() =>
  zql.webhook.related('workspace', q => q.related('teams')),
);
const q17 = defineQueryTyped(() =>
  zql.webhookDelivery.related('workspace', q => q.related('teams')),
);
const q18 = defineQueryTyped(() =>
  zql.rateLimit.related('workspace', q => q.related('teams')),
);
const q19 = defineQueryTyped(() =>
  zql.featureFlag.related('workspace', q => q.related('teams')),
);
const q20 = defineQueryTyped(() =>
  zql.integration.related('workspace', q => q.related('teams')),
);
const q21 = defineQueryTyped(() =>
  zql.emailCampaign.related('workspace', q => q.related('teams')),
);
const q22 = defineQueryTyped(() =>
  zql.emailTemplate.related('workspace', q => q.related('teams')),
);
// Adding any more queries causes:
// `The inferred type of this node exceeds the maximum length the compiler will serialize.`
// const q23 = defineQueryTyped(() =>
//   zql.emailSend.related('workspace', q => q.related('teams')),
// );
// const q24 = defineQueryTyped(() =>
//   zql.subscriberList.related('workspace', q => q.related('teams')),
// );

const queries = defineQueries({
  wide: queryWide,
  deep: queryDeep,
  q01,
  q02,
  q03,
  q04,
  q05,
  q06,
  q07,
  q08,
  q09,
  q10,
  q11,
  q12,
  q13,
  q14,
  q15,
  q16,
  q17,
  q18,
  q19,
  q20,
  q21,
  q22,
  // q23,
  // q24,
});

// this is testing .d.ts generation for queries
export {queries};
