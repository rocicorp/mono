// This file defines complex mutators to stress test TypeScript's type
// inference.

import type {StandardSchemaV1} from '@standard-schema/spec';
import type {InsertValue} from '../../../zql/src/mutate/custom.ts';
import {defineMutators} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../../zql/src/mutate/mutator.ts';
import type {zeroStressSchema} from './zero-stress-schema-test.ts';

export type StressContext = {
  someUserId: string;
  role: 'admin' | 'user';
  workspaceId: string;
  workspaceName: string;
};

export type StressTransaction = {
  db: true;
};

const m = {
  // Basic insert operations
  updateThing: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.vitalSigns>,
    InsertValue<typeof zeroStressSchema.tables.vitalSigns>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.vitalSigns>,
      InsertValue<typeof zeroStressSchema.tables.vitalSigns>
    >,
    async ({tx, args}) => {
      await tx.mutate.vitalSigns.insert(args);
    },
  ),

  createUser: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.user>,
    InsertValue<typeof zeroStressSchema.tables.user>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.user>,
      InsertValue<typeof zeroStressSchema.tables.user>
    >,
    async ({tx, args}) => {
      await tx.mutate.user.insert(args);
    },
  ),

  insertProduct: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.product>,
    InsertValue<typeof zeroStressSchema.tables.product>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.product>,
      InsertValue<typeof zeroStressSchema.tables.product>
    >,
    async ({tx, args}) => {
      await tx.mutate.product.insert(args);
    },
  ),

  addOrder: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.order>,
    InsertValue<typeof zeroStressSchema.tables.order>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.order>,
      InsertValue<typeof zeroStressSchema.tables.order>
    >,
    async ({tx, args}) => {
      await tx.mutate.order.insert(args);
    },
  ),

  createTicket: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.supportTicket>,
    InsertValue<typeof zeroStressSchema.tables.supportTicket>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.supportTicket>,
      InsertValue<typeof zeroStressSchema.tables.supportTicket>
    >,
    async ({tx, args}) => {
      await tx.mutate.supportTicket.insert(args);
    },
  ),

  insertPatient: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.patient>,
    InsertValue<typeof zeroStressSchema.tables.patient>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.patient>,
      InsertValue<typeof zeroStressSchema.tables.patient>
    >,
    async ({tx, args}) => {
      await tx.mutate.patient.insert(args);
    },
  ),

  createProject: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.project>,
    InsertValue<typeof zeroStressSchema.tables.project>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.project>,
      InsertValue<typeof zeroStressSchema.tables.project>
    >,
    async ({tx, args}) => {
      await tx.mutate.project.insert(args);
    },
  ),

  addTask: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.task>,
    InsertValue<typeof zeroStressSchema.tables.task>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.task>,
      InsertValue<typeof zeroStressSchema.tables.task>
    >,
    async ({tx, args}) => {
      await tx.mutate.task.insert(args);
    },
  ),

  createEmployee: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.employee>,
    InsertValue<typeof zeroStressSchema.tables.employee>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.employee>,
      InsertValue<typeof zeroStressSchema.tables.employee>
    >,
    async ({tx, args}) => {
      await tx.mutate.employee.insert(args);
    },
  ),

  insertInvoice: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.invoice>,
    InsertValue<typeof zeroStressSchema.tables.invoice>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.invoice>,
      InsertValue<typeof zeroStressSchema.tables.invoice>
    >,
    async ({tx, args}) => {
      await tx.mutate.invoice.insert(args);
    },
  ),

  addWorkspace: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.workspace>,
    InsertValue<typeof zeroStressSchema.tables.workspace>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.workspace>,
      InsertValue<typeof zeroStressSchema.tables.workspace>
    >,
    async ({tx, args}) => {
      await tx.mutate.workspace.insert(args);
    },
  ),

  createSession: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.session>,
    InsertValue<typeof zeroStressSchema.tables.session>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.session>,
      InsertValue<typeof zeroStressSchema.tables.session>
    >,
    async ({tx, args}) => {
      await tx.mutate.session.insert(args);
    },
  ),

  addEmailCampaign: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.emailCampaign>,
    InsertValue<typeof zeroStressSchema.tables.emailCampaign>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.emailCampaign>,
      InsertValue<typeof zeroStressSchema.tables.emailCampaign>
    >,
    async ({tx, args}) => {
      await tx.mutate.emailCampaign.insert(args);
    },
  ),

  insertAppointment: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.appointment>,
    InsertValue<typeof zeroStressSchema.tables.appointment>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.appointment>,
      InsertValue<typeof zeroStressSchema.tables.appointment>
    >,
    async ({tx, args}) => {
      await tx.mutate.appointment.insert(args);
    },
  ),

  createWebhook: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.webhook>,
    InsertValue<typeof zeroStressSchema.tables.webhook>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.webhook>,
      InsertValue<typeof zeroStressSchema.tables.webhook>
    >,
    async ({tx, args}) => {
      await tx.mutate.webhook.insert(args);
    },
  ),

  addAuditLog: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.auditLog>,
    InsertValue<typeof zeroStressSchema.tables.auditLog>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.auditLog>,
      InsertValue<typeof zeroStressSchema.tables.auditLog>
    >,
    async ({tx, args}) => {
      await tx.mutate.auditLog.insert(args);
    },
  ),

  insertTeam: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.team>,
    InsertValue<typeof zeroStressSchema.tables.team>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.team>,
      InsertValue<typeof zeroStressSchema.tables.team>
    >,
    async ({tx, args}) => {
      await tx.mutate.team.insert(args);
    },
  ),

  createSprint: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.sprint>,
    InsertValue<typeof zeroStressSchema.tables.sprint>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.sprint>,
      InsertValue<typeof zeroStressSchema.tables.sprint>
    >,
    async ({tx, args}) => {
      await tx.mutate.sprint.insert(args);
    },
  ),

  addPayrollRun: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.payrollRun>,
    InsertValue<typeof zeroStressSchema.tables.payrollRun>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.payrollRun>,
      InsertValue<typeof zeroStressSchema.tables.payrollRun>
    >,
    async ({tx, args}) => {
      await tx.mutate.payrollRun.insert(args);
    },
  ),

  insertBudget: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.budget>,
    InsertValue<typeof zeroStressSchema.tables.budget>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.budget>,
      InsertValue<typeof zeroStressSchema.tables.budget>
    >,
    async ({tx, args}) => {
      await tx.mutate.budget.insert(args);
    },
  ),

  createPrescription: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.prescription>,
    InsertValue<typeof zeroStressSchema.tables.prescription>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.prescription>,
      InsertValue<typeof zeroStressSchema.tables.prescription>
    >,
    async ({tx, args}) => {
      await tx.mutate.prescription.insert(args);
    },
  ),

  // Update operations
  updateUser: defineMutator<
    {workspaceId: string; userId: string; name: string},
    {workspaceId: string; userId: string; name: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; userId: string; name: string},
      {workspaceId: string; userId: string; name: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.user.update({
        workspaceId: args.workspaceId,
        userId: args.userId,
      });
    },
  ),

  updateProductStatus: defineMutator<
    {workspaceId: string; productId: string; status: string},
    {workspaceId: string; productId: string; status: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; productId: string; status: string},
      {workspaceId: string; productId: string; status: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.product.update({
        workspaceId: args.workspaceId,
        productId: args.productId,
      });
    },
  ),

  updateTicketPriority: defineMutator<
    {workspaceId: string; ticketId: string; priority: string},
    {workspaceId: string; ticketId: string; priority: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; ticketId: string; priority: string},
      {workspaceId: string; ticketId: string; priority: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.supportTicket.update({
        workspaceId: args.workspaceId,
        ticketId: args.ticketId,
      });
    },
  ),

  updateTaskStatus: defineMutator<
    {workspaceId: string; taskId: string; status: string},
    {workspaceId: string; taskId: string; status: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; taskId: string; status: string},
      {workspaceId: string; taskId: string; status: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.task.update({
        workspaceId: args.workspaceId,
        taskId: args.taskId,
      });
    },
  ),

  updateOrderStatus: defineMutator<
    {workspaceId: string; orderId: string; status: string},
    {workspaceId: string; orderId: string; status: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; orderId: string; status: string},
      {workspaceId: string; orderId: string; status: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.order.update({
        workspaceId: args.workspaceId,
        orderId: args.orderId,
      });
    },
  ),

  // Delete operations
  deleteSession: defineMutator<
    {workspaceId: string; sessionId: string},
    {workspaceId: string; sessionId: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; sessionId: string},
      {workspaceId: string; sessionId: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.session.delete({
        workspaceId: args.workspaceId,
        sessionId: args.sessionId,
      });
    },
  ),

  deleteWebhook: defineMutator<
    {workspaceId: string; webhookId: string},
    {workspaceId: string; webhookId: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; webhookId: string},
      {workspaceId: string; webhookId: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.webhook.delete({
        workspaceId: args.workspaceId,
        webhookId: args.webhookId,
      });
    },
  ),

  deleteTask: defineMutator<
    {workspaceId: string; taskId: string},
    {workspaceId: string; taskId: string},
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {workspaceId: string; taskId: string},
      {workspaceId: string; taskId: string}
    >,
    async ({tx, args}) => {
      await tx.mutate.task.delete({
        workspaceId: args.workspaceId,
        taskId: args.taskId,
      });
    },
  ),

  // Upsert operations
  upsertProduct: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.product>,
    InsertValue<typeof zeroStressSchema.tables.product>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.product>,
      InsertValue<typeof zeroStressSchema.tables.product>
    >,
    async ({tx, args}) => {
      await tx.mutate.product.upsert(args);
    },
  ),

  upsertEmployee: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.employee>,
    InsertValue<typeof zeroStressSchema.tables.employee>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.employee>,
      InsertValue<typeof zeroStressSchema.tables.employee>
    >,
    async ({tx, args}) => {
      await tx.mutate.employee.upsert(args);
    },
  ),

  upsertWorkspace: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.workspace>,
    InsertValue<typeof zeroStressSchema.tables.workspace>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.workspace>,
      InsertValue<typeof zeroStressSchema.tables.workspace>
    >,
    async ({tx, args}) => {
      await tx.mutate.workspace.upsert(args);
    },
  ),

  // Complex multi-operation mutators
  createOrderWithLineItems: defineMutator<
    {
      order: InsertValue<typeof zeroStressSchema.tables.order>;
      lineItems: InsertValue<typeof zeroStressSchema.tables.orderLineItem>[];
    },
    {
      order: InsertValue<typeof zeroStressSchema.tables.order>;
      lineItems: InsertValue<typeof zeroStressSchema.tables.orderLineItem>[];
    },
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {
        order: InsertValue<typeof zeroStressSchema.tables.order>;
        lineItems: InsertValue<typeof zeroStressSchema.tables.orderLineItem>[];
      },
      {
        order: InsertValue<typeof zeroStressSchema.tables.order>;
        lineItems: InsertValue<typeof zeroStressSchema.tables.orderLineItem>[];
      }
    >,
    async ({tx, args}) => {
      await tx.mutate.order.insert(args.order);
      for (const item of args.lineItems) {
        await tx.mutate.orderLineItem.insert(item);
      }
    },
  ),

  createProjectWithTasks: defineMutator<
    {
      project: InsertValue<typeof zeroStressSchema.tables.project>;
      tasks: InsertValue<typeof zeroStressSchema.tables.task>[];
    },
    {
      project: InsertValue<typeof zeroStressSchema.tables.project>;
      tasks: InsertValue<typeof zeroStressSchema.tables.task>[];
    },
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {
        project: InsertValue<typeof zeroStressSchema.tables.project>;
        tasks: InsertValue<typeof zeroStressSchema.tables.task>[];
      },
      {
        project: InsertValue<typeof zeroStressSchema.tables.project>;
        tasks: InsertValue<typeof zeroStressSchema.tables.task>[];
      }
    >,
    async ({tx, args}) => {
      await tx.mutate.project.insert(args.project);
      for (const task of args.tasks) {
        await tx.mutate.task.insert(task);
      }
    },
  ),

  addPatientWithAppointment: defineMutator<
    {
      patient: InsertValue<typeof zeroStressSchema.tables.patient>;
      appointment: InsertValue<typeof zeroStressSchema.tables.appointment>;
    },
    {
      patient: InsertValue<typeof zeroStressSchema.tables.patient>;
      appointment: InsertValue<typeof zeroStressSchema.tables.appointment>;
    },
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      {
        patient: InsertValue<typeof zeroStressSchema.tables.patient>;
        appointment: InsertValue<typeof zeroStressSchema.tables.appointment>;
      },
      {
        patient: InsertValue<typeof zeroStressSchema.tables.patient>;
        appointment: InsertValue<typeof zeroStressSchema.tables.appointment>;
      }
    >,
    async ({tx, args}) => {
      await tx.mutate.patient.insert(args.patient);
      await tx.mutate.appointment.insert(args.appointment);
    },
  ),

  // Mutators using context
  // createUserWithContext: defineMutator(
  //   ((v: unknown) => v) as unknown as StandardSchemaV1<
  //     {email: string; name: string},
  //     {email: string; name: string}
  //   >,
  //   async ({tx, args, ctx}) => {
  //     await tx.mutate.user.insert({
  //       workspaceId: ctx.workspaceId,
  //       userId: ctx.someUserId,
  //       email: args.email,
  //       name: args.name,
  //       emailVerified: false,
  //       role: ctx.role === 'admin' ? 'admin' : 'member',
  //       status: 'active',
  //       timezone: 'UTC',
  //       locale: 'en-US',
  //       twoFactorEnabled: false,
  //       passwordHash: 'hash',
  //       metadata: {
  //         preferences: {theme: 'dark'},
  //         onboarding: {completed: false, step: 0},
  //       },
  //       activityData: {
  //         type: 'login',
  //         timestamp: Date.now(),
  //         ip: '0.0.0.0',
  //         device: 'browser',
  //       },
  //       createdAt: Date.now(),
  //       updatedAt: Date.now(),
  //     });
  //   },
  // ),

  // Additional table coverage
  createFeatureFlag: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.featureFlag>,
    InsertValue<typeof zeroStressSchema.tables.featureFlag>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.featureFlag>,
      InsertValue<typeof zeroStressSchema.tables.featureFlag>
    >,
    async ({tx, args}) => {
      await tx.mutate.featureFlag.insert(args);
    },
  ),

  addInventoryAdjustment: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.inventoryAdjustment>,
    InsertValue<typeof zeroStressSchema.tables.inventoryAdjustment>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.inventoryAdjustment>,
      InsertValue<typeof zeroStressSchema.tables.inventoryAdjustment>
    >,
    async ({tx, args}) => {
      await tx.mutate.inventoryAdjustment.insert(args);
    },
  ),

  createMedicalRecord: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.medicalRecord>,
    InsertValue<typeof zeroStressSchema.tables.medicalRecord>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.medicalRecord>,
      InsertValue<typeof zeroStressSchema.tables.medicalRecord>
    >,
    async ({tx, args}) => {
      await tx.mutate.medicalRecord.insert(args);
    },
  ),

  insertLabOrder: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.labOrder>,
    InsertValue<typeof zeroStressSchema.tables.labOrder>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.labOrder>,
      InsertValue<typeof zeroStressSchema.tables.labOrder>
    >,
    async ({tx, args}) => {
      await tx.mutate.labOrder.insert(args);
    },
  ),

  createTimeEntry: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.timeEntry>,
    InsertValue<typeof zeroStressSchema.tables.timeEntry>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.timeEntry>,
      InsertValue<typeof zeroStressSchema.tables.timeEntry>
    >,
    async ({tx, args}) => {
      await tx.mutate.timeEntry.insert(args);
    },
  ),

  addMilestone: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.milestone>,
    InsertValue<typeof zeroStressSchema.tables.milestone>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.milestone>,
      InsertValue<typeof zeroStressSchema.tables.milestone>
    >,
    async ({tx, args}) => {
      await tx.mutate.milestone.insert(args);
    },
  ),

  createJournalEntry: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.journalEntry>,
    InsertValue<typeof zeroStressSchema.tables.journalEntry>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.journalEntry>,
      InsertValue<typeof zeroStressSchema.tables.journalEntry>
    >,
    async ({tx, args}) => {
      await tx.mutate.journalEntry.insert(args);
    },
  ),

  insertPayment: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.payment>,
    InsertValue<typeof zeroStressSchema.tables.payment>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.payment>,
      InsertValue<typeof zeroStressSchema.tables.payment>
    >,
    async ({tx, args}) => {
      await tx.mutate.payment.insert(args);
    },
  ),

  createCmsArticle: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.cmsArticle>,
    InsertValue<typeof zeroStressSchema.tables.cmsArticle>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.cmsArticle>,
      InsertValue<typeof zeroStressSchema.tables.cmsArticle>
    >,
    async ({tx, args}) => {
      await tx.mutate.cmsArticle.insert(args);
    },
  ),

  addDiscountCode: defineMutator<
    InsertValue<typeof zeroStressSchema.tables.discountCode>,
    InsertValue<typeof zeroStressSchema.tables.discountCode>,
    typeof zeroStressSchema,
    StressContext,
    StressTransaction
  >(
    ((v: unknown) => v) as unknown as StandardSchemaV1<
      InsertValue<typeof zeroStressSchema.tables.discountCode>,
      InsertValue<typeof zeroStressSchema.tables.discountCode>
    >,
    async ({tx, args}) => {
      await tx.mutate.discountCode.insert(args);
    },
  ),
};

const mutators = defineMutators<
  typeof m,
  typeof zeroStressSchema,
  StressContext
>(m);

// this is testing .d.ts generation for complex mutators
export {mutators};
