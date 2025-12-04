// This file defines complex mutators to stress test TypeScript's type
// inference.

import type {StandardSchemaV1} from '@standard-schema/spec';
import type {InsertValue} from '../../../zql/src/mutate/custom.ts';
import {defineMutators} from '../../../zql/src/mutate/mutator-registry.ts';
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

const mutators = defineMutators({
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
});

// this is testing .d.ts generation for complex mutators
export {mutators};
