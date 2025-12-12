// This file defines 100 mutators to stress test TS

import type {StandardSchemaV1} from '@standard-schema/spec';
import type {InsertValue} from '../../../zql/src/mutate/crud.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../../zql/src/mutate/mutator.ts';
import type {zeroStressSchema} from './zero-stress-schema-test.ts';
import type {
  StressContext,
  StressTransaction,
} from './zero-stress-shared-test.ts';

const defineMutatorTyped = defineMutatorWithType<
  typeof zeroStressSchema,
  StressContext,
  StressTransaction
>();

const mutators = defineMutatorsWithType<typeof zeroStressSchema>()({
  // Basic insert operations
  updateThing: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.vitalSigns>
    >,
    async ({tx, args}) => {
      await tx.mutate.vitalSigns.insert(args);
    },
  ),

  createUser: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.user>
    >,
    async ({tx, args}) => {
      await tx.mutate.user.insert(args);
    },
  ),

  insertProduct: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.product>
    >,
    async ({tx, args}) => {
      await tx.mutate.product.insert(args);
    },
  ),

  addOrder: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.order>
    >,
    async ({tx, args}) => {
      await tx.mutate.order.insert(args);
    },
  ),

  createTicket: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.supportTicket>
    >,
    async ({tx, args}) => {
      await tx.mutate.supportTicket.insert(args);
    },
  ),

  insertPatient: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.patient>
    >,
    async ({tx, args}) => {
      await tx.mutate.patient.insert(args);
    },
  ),

  createProject: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.project>
    >,
    async ({tx, args}) => {
      await tx.mutate.project.insert(args);
    },
  ),

  addTask: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.task>
    >,
    async ({tx, args}) => {
      await tx.mutate.task.insert(args);
    },
  ),

  createEmployee: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.employee>
    >,
    async ({tx, args}) => {
      await tx.mutate.employee.insert(args);
    },
  ),

  insertInvoice: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.invoice>
    >,
    async ({tx, args}) => {
      await tx.mutate.invoice.insert(args);
    },
  ),

  addWorkspace: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.workspace>
    >,
    async ({tx, args}) => {
      await tx.mutate.workspace.insert(args);
    },
  ),

  createSession: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.session>
    >,
    async ({tx, args}) => {
      await tx.mutate.session.insert(args);
    },
  ),

  addEmailCampaign: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.emailCampaign>
    >,
    async ({tx, args}) => {
      await tx.mutate.emailCampaign.insert(args);
    },
  ),

  insertAppointment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.appointment>
    >,
    async ({tx, args}) => {
      await tx.mutate.appointment.insert(args);
    },
  ),

  createWebhook: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.webhook>
    >,
    async ({tx, args}) => {
      await tx.mutate.webhook.insert(args);
    },
  ),

  addAuditLog: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.auditLog>
    >,
    async ({tx, args}) => {
      await tx.mutate.auditLog.insert(args);
    },
  ),

  insertTeam: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.team>
    >,
    async ({tx, args}) => {
      await tx.mutate.team.insert(args);
    },
  ),

  createSprint: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.sprint>
    >,
    async ({tx, args}) => {
      await tx.mutate.sprint.insert(args);
    },
  ),

  addPayrollRun: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.payrollRun>
    >,
    async ({tx, args}) => {
      await tx.mutate.payrollRun.insert(args);
    },
  ),

  insertBudget: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.budget>
    >,
    async ({tx, args}) => {
      await tx.mutate.budget.insert(args);
    },
  ),

  createPrescription: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.prescription>
    >,
    async ({tx, args}) => {
      await tx.mutate.prescription.insert(args);
    },
  ),

  // Update operations
  updateUser: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      userId: string;
      name: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.user.update({
        workspaceId: args.workspaceId,
        userId: args.userId,
      });
    },
  ),

  updateProductStatus: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      productId: string;
      status: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.product.update({
        workspaceId: args.workspaceId,
        productId: args.productId,
      });
    },
  ),

  updateTicketPriority: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      ticketId: string;
      priority: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.supportTicket.update({
        workspaceId: args.workspaceId,
        ticketId: args.ticketId,
      });
    },
  ),

  updateTaskStatus: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      taskId: string;
      status: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.task.update({
        workspaceId: args.workspaceId,
        taskId: args.taskId,
      });
    },
  ),

  updateOrderStatus: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      orderId: string;
      status: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.order.update({
        workspaceId: args.workspaceId,
        orderId: args.orderId,
      });
    },
  ),

  // Delete operations
  deleteSession: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      sessionId: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.session.delete({
        workspaceId: args.workspaceId,
        sessionId: args.sessionId,
      });
    },
  ),

  deleteWebhook: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      webhookId: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.webhook.delete({
        workspaceId: args.workspaceId,
        webhookId: args.webhookId,
      });
    },
  ),

  deleteTask: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      taskId: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.task.delete({
        workspaceId: args.workspaceId,
        taskId: args.taskId,
      });
    },
  ),

  // Upsert operations
  upsertProduct: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.product>
    >,
    async ({tx, args}) => {
      await tx.mutate.product.upsert(args);
    },
  ),

  upsertEmployee: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.employee>
    >,
    async ({tx, args}) => {
      await tx.mutate.employee.upsert(args);
    },
  ),

  upsertWorkspace: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.workspace>
    >,
    async ({tx, args}) => {
      await tx.mutate.workspace.upsert(args);
    },
  ),

  // Complex multi-operation mutators
  createOrderWithLineItems: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      order: InsertValue<typeof zeroStressSchema.tables.order>;
      lineItems: InsertValue<typeof zeroStressSchema.tables.orderLineItem>[];
    }>,
    async ({tx, args}) => {
      await tx.mutate.order.insert(args.order);
      for (const item of args.lineItems) {
        await tx.mutate.orderLineItem.insert(item);
      }
    },
  ),

  createProjectWithTasks: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      project: InsertValue<typeof zeroStressSchema.tables.project>;
      tasks: InsertValue<typeof zeroStressSchema.tables.task>[];
    }>,
    async ({tx, args}) => {
      await tx.mutate.project.insert(args.project);
      for (const task of args.tasks) {
        await tx.mutate.task.insert(task);
      }
    },
  ),

  addPatientWithAppointment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      patient: InsertValue<typeof zeroStressSchema.tables.patient>;
      appointment: InsertValue<typeof zeroStressSchema.tables.appointment>;
    }>,
    async ({tx, args}) => {
      await tx.mutate.patient.insert(args.patient);
      await tx.mutate.appointment.insert(args.appointment);
    },
  ),

  // Mutators using context
  createUserWithContext: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      email: string;
      name: string;
    }>,
    async ({tx, args, ctx}) => {
      await tx.mutate.user.insert({
        workspaceId: ctx.workspaceId ?? 'workspaceId',
        userId: ctx.userId ?? 'userId',
        email: args.email,
        name: args.name,
        emailVerified: false,
        role: ctx.role === 'admin' ? 'admin' : 'member',
        status: 'active',
        timezone: 'UTC',
        locale: 'en-US',
        twoFactorEnabled: false,
        passwordHash: 'hash',
        metadata: {
          preferences: {theme: 'dark'},
          onboarding: {completed: false, step: 0},
        },
        activityData: {
          type: 'login',
          timestamp: Date.now(),
          ip: '0.0.0.0',
          device: 'browser',
        },
        createdAt: Date.now(),
        updatedAt: Date.now(),
      });
    },
  ),

  // Additional table coverage
  createFeatureFlag: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.featureFlag>
    >,
    async ({tx, args}) => {
      await tx.mutate.featureFlag.insert(args);
    },
  ),

  addInventoryAdjustment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.inventoryAdjustment>
    >,
    async ({tx, args}) => {
      await tx.mutate.inventoryAdjustment.insert(args);
    },
  ),

  createMedicalRecord: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.medicalRecord>
    >,
    async ({tx, args}) => {
      await tx.mutate.medicalRecord.insert(args);
    },
  ),

  insertLabOrder: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.labOrder>
    >,
    async ({tx, args}) => {
      await tx.mutate.labOrder.insert(args);
    },
  ),

  createTimeEntry: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.timeEntry>
    >,
    async ({tx, args}) => {
      await tx.mutate.timeEntry.insert(args);
    },
  ),

  addMilestone: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.milestone>
    >,
    async ({tx, args}) => {
      await tx.mutate.milestone.insert(args);
    },
  ),

  createJournalEntry: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.journalEntry>
    >,
    async ({tx, args}) => {
      await tx.mutate.journalEntry.insert(args);
    },
  ),

  insertPayment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.payment>
    >,
    async ({tx, args}) => {
      await tx.mutate.payment.insert(args);
    },
  ),

  createCmsArticle: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.cmsArticle>
    >,
    async ({tx, args}) => {
      await tx.mutate.cmsArticle.insert(args);
    },
  ),

  addDiscountCode: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.discountCode>
    >,
    async ({tx, args}) => {
      await tx.mutate.discountCode.insert(args);
    },
  ),

  // Additional mutators to reach 100 total (54 more)
  createAccount: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.account>
    >,
    async ({tx, args}) => {
      await tx.mutate.account.insert(args);
    },
  ),

  addWorkspaceMember: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.workspaceMember>
    >,
    async ({tx, args}) => {
      await tx.mutate.workspaceMember.insert(args);
    },
  ),

  insertApiKey: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.apiKey>
    >,
    async ({tx, args}) => {
      await tx.mutate.apiKey.insert(args);
    },
  ),

  createVerificationToken: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.verificationToken>
    >,
    async ({tx, args}) => {
      await tx.mutate.verificationToken.insert(args);
    },
  ),

  addPasswordReset: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.passwordReset>
    >,
    async ({tx, args}) => {
      await tx.mutate.passwordReset.insert(args);
    },
  ),

  createEntityTag: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.entityTag>
    >,
    async ({tx, args}) => {
      await tx.mutate.entityTag.insert(args);
    },
  ),

  addEntityAttachment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.entityAttachment>
    >,
    async ({tx, args}) => {
      await tx.mutate.entityAttachment.insert(args);
    },
  ),

  insertEntityComment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.entityComment>
    >,
    async ({tx, args}) => {
      await tx.mutate.entityComment.insert(args);
    },
  ),

  createCustomFieldDefinition: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.customFieldDefinition>
    >,
    async ({tx, args}) => {
      await tx.mutate.customFieldDefinition.insert(args);
    },
  ),

  addCustomFieldValue: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.customFieldValue>
    >,
    async ({tx, args}) => {
      await tx.mutate.customFieldValue.insert(args);
    },
  ),

  insertWebhookDelivery: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.webhookDelivery>
    >,
    async ({tx, args}) => {
      await tx.mutate.webhookDelivery.insert(args);
    },
  ),

  createRateLimit: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.rateLimit>
    >,
    async ({tx, args}) => {
      await tx.mutate.rateLimit.insert(args);
    },
  ),

  addIntegration: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.integration>
    >,
    async ({tx, args}) => {
      await tx.mutate.integration.insert(args);
    },
  ),

  createEmailTemplate: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.emailTemplate>
    >,
    async ({tx, args}) => {
      await tx.mutate.emailTemplate.insert(args);
    },
  ),

  insertEmailSend: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.emailSend>
    >,
    async ({tx, args}) => {
      await tx.mutate.emailSend.insert(args);
    },
  ),

  addSubscriberList: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.subscriberList>
    >,
    async ({tx, args}) => {
      await tx.mutate.subscriberList.insert(args);
    },
  ),

  createSubscriber: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.subscriber>
    >,
    async ({tx, args}) => {
      await tx.mutate.subscriber.insert(args);
    },
  ),

  insertAutomationWorkflow: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.automationWorkflow>
    >,
    async ({tx, args}) => {
      await tx.mutate.automationWorkflow.insert(args);
    },
  ),

  addEmailLink: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.emailLink>
    >,
    async ({tx, args}) => {
      await tx.mutate.emailLink.insert(args);
    },
  ),

  createUnsubscribeEvent: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.unsubscribeEvent>
    >,
    async ({tx, args}) => {
      await tx.mutate.unsubscribeEvent.insert(args);
    },
  ),

  insertEmailAttachment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.emailAttachment>
    >,
    async ({tx, args}) => {
      await tx.mutate.emailAttachment.insert(args);
    },
  ),

  addSpamComplaint: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.spamComplaint>
    >,
    async ({tx, args}) => {
      await tx.mutate.spamComplaint.insert(args);
    },
  ),

  createTicketMessage: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.ticketMessage>
    >,
    async ({tx, args}) => {
      await tx.mutate.ticketMessage.insert(args);
    },
  ),

  insertKnowledgeBaseArticle: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.knowledgeBaseArticle>
    >,
    async ({tx, args}) => {
      await tx.mutate.knowledgeBaseArticle.insert(args);
    },
  ),

  addSlaPolicy: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.slaPolicy>
    >,
    async ({tx, args}) => {
      await tx.mutate.slaPolicy.insert(args);
    },
  ),

  createCannedResponse: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.cannedResponse>
    >,
    async ({tx, args}) => {
      await tx.mutate.cannedResponse.insert(args);
    },
  ),

  insertTicketTag: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.ticketTag>
    >,
    async ({tx, args}) => {
      await tx.mutate.ticketTag.insert(args);
    },
  ),

  addSatisfactionSurvey: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.satisfactionSurvey>
    >,
    async ({tx, args}) => {
      await tx.mutate.satisfactionSurvey.insert(args);
    },
  ),

  createAgentAssignment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.agentAssignment>
    >,
    async ({tx, args}) => {
      await tx.mutate.agentAssignment.insert(args);
    },
  ),

  insertTicketEscalation: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.ticketEscalation>
    >,
    async ({tx, args}) => {
      await tx.mutate.ticketEscalation.insert(args);
    },
  ),

  addProductVariant: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.productVariant>
    >,
    async ({tx, args}) => {
      await tx.mutate.productVariant.insert(args);
    },
  ),

  createShoppingCart: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.shoppingCart>
    >,
    async ({tx, args}) => {
      await tx.mutate.shoppingCart.insert(args);
    },
  ),

  insertProductReview: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.productReview>
    >,
    async ({tx, args}) => {
      await tx.mutate.productReview.insert(args);
    },
  ),

  addShippingZone: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.shippingZone>
    >,
    async ({tx, args}) => {
      await tx.mutate.shippingZone.insert(args);
    },
  ),

  createPaymentTransaction: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.paymentTransaction>
    >,
    async ({tx, args}) => {
      await tx.mutate.paymentTransaction.insert(args);
    },
  ),

  insertCmsPage: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.cmsPage>
    >,
    async ({tx, args}) => {
      await tx.mutate.cmsPage.insert(args);
    },
  ),

  addMediaAsset: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.mediaAsset>
    >,
    async ({tx, args}) => {
      await tx.mutate.mediaAsset.insert(args);
    },
  ),

  createContentRevision: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.contentRevision>
    >,
    async ({tx, args}) => {
      await tx.mutate.contentRevision.insert(args);
    },
  ),

  insertTaxonomyTerm: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.taxonomyTerm>
    >,
    async ({tx, args}) => {
      await tx.mutate.taxonomyTerm.insert(args);
    },
  ),

  addContentBlock: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.contentBlock>
    >,
    async ({tx, args}) => {
      await tx.mutate.contentBlock.insert(args);
    },
  ),

  createCmsMenu: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.cmsMenu>
    >,
    async ({tx, args}) => {
      await tx.mutate.cmsMenu.insert(args);
    },
  ),

  insertRedirectRule: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.redirectRule>
    >,
    async ({tx, args}) => {
      await tx.mutate.redirectRule.insert(args);
    },
  ),

  addCmsComment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.cmsComment>
    >,
    async ({tx, args}) => {
      await tx.mutate.cmsComment.insert(args);
    },
  ),

  createTaskDependency: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.taskDependency>
    >,
    async ({tx, args}) => {
      await tx.mutate.taskDependency.insert(args);
    },
  ),

  insertBoard: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.board>
    >,
    async ({tx, args}) => {
      await tx.mutate.board.insert(args);
    },
  ),

  addTaskComment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.taskComment>
    >,
    async ({tx, args}) => {
      await tx.mutate.taskComment.insert(args);
    },
  ),

  createDepartment: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.department>
    >,
    async ({tx, args}) => {
      await tx.mutate.department.insert(args);
    },
  ),

  insertLabResult: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.labResult>
    >,
    async ({tx, args}) => {
      await tx.mutate.labResult.insert(args);
    },
  ),

  addDiagnosis: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.diagnosis>
    >,
    async ({tx, args}) => {
      await tx.mutate.diagnosis.insert(args);
    },
  ),

  createImmunization: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.immunization>
    >,
    async ({tx, args}) => {
      await tx.mutate.immunization.insert(args);
    },
  ),

  // Update operations
  updateApiKey: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      keyId: string;
      status: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.apiKey.update({
        workspaceId: args.workspaceId,
        keyId: args.keyId,
      });
    },
  ),

  updateIntegration: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      integrationId: string;
      enabled: boolean;
    }>,
    async ({tx, args}) => {
      await tx.mutate.integration.update({
        workspaceId: args.workspaceId,
        integrationId: args.integrationId,
      });
    },
  ),

  // Delete operations
  deleteWorkspaceMember: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      memberId: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.workspaceMember.delete({
        workspaceId: args.workspaceId,
        memberId: args.memberId,
      });
    },
  ),

  deleteVerificationToken: defineMutatorTyped(
    ((v: unknown) => v) as unknown as StandardSchemaV1<{
      workspaceId: string;
      tokenId: string;
    }>,
    async ({tx, args}) => {
      await tx.mutate.verificationToken.delete({
        workspaceId: args.workspaceId,
        tokenId: args.tokenId,
      });
    },
  ),
});

// this is testing .d.ts generation for complex mutators
export {mutators};
