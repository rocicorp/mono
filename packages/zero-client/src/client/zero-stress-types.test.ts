import {describe, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {mustGetMutator} from '../../../zql/src/mutate/mutator-registry.ts';
import type {Mutator} from '../../../zql/src/mutate/mutator.ts';
import {mustGetQuery} from '../../../zql/src/query/query-registry.ts';
import type {QueryResultType} from '../../../zql/src/query/query.ts';
import type {MutatorResultDetails} from './custom.ts';
import {zeroStress} from './zero-stress-client-test.ts';
import {mutators} from './zero-stress-mutators-test.ts';
import {queries} from './zero-stress-queries-test.ts';
import {type zeroStressSchema, zql} from './zero-stress-schema-test.ts';
import type {
  StressContext,
  StressTransaction,
} from './zero-stress-shared-test.ts';

describe('stress test types', () => {
  test('zero can resolve query return types', async () => {
    const result = await zeroStress.run(
      zql.abTest.where('endDate', '>', 1726339646439),
    );

    expectTypeOf(result).toEqualTypeOf<
      {
        readonly workspaceId: string;
        readonly testId: string;
        readonly testName: string;
        readonly description: string | null;
        readonly variants: readonly {
          readonly variantId: string;
          readonly name: string;
          readonly content: {
            readonly [key: string]: string;
          };
          readonly weight: number;
        }[];
        readonly trafficSplit: {
          readonly [variantId: string]: number;
        };
        readonly startDate: number;
        readonly endDate: number | null;
        readonly metricTracked: string;
        readonly winningVariant: string | null;
        readonly statisticalSignificance: number | null;
        readonly status: 'draft' | 'running' | 'paused' | 'completed';
        readonly createdById: string;
        readonly createdAt: number;
        readonly updatedAt: number;
      }[]
    >();
  });

  test('can resolve mutator types', () => {
    const mutator = mustGetMutator(mutators, 'updateThing');
    expectTypeOf<typeof mutator>().toEqualTypeOf<
      Mutator<
        ReadonlyJSONValue | undefined,
        typeof zeroStressSchema,
        StressContext,
        StressTransaction
      >
    >();
    expectTypeOf<typeof mutators.updateThing>().toEqualTypeOf<
      Mutator<
        {
          workspaceId: string;
          vitalId: string;
          readonly bloodPressureSystolic?: number | null | undefined;
          readonly bloodPressureDiastolic?: number | null | undefined;
          readonly heartRate?: number | null | undefined;
          readonly temperature?: number | null | undefined;
          readonly weight?: number | null | undefined;
          readonly height?: number | null | undefined;
          readonly oxygenSaturation?: number | null | undefined;
          readonly patientId: string;
          readonly recordedAt: string;
          readonly recordedById: string;
          readonly createdAt: number;
        },
        typeof zeroStressSchema,
        StressContext,
        StressTransaction
      >
    >();
  });

  test('can resolve mutation types', () => {
    expectTypeOf<
      Awaited<ReturnType<typeof zeroStress.mutate>['client']>
    >().toEqualTypeOf<MutatorResultDetails>();
  });

  test('can resolve query types', () => {
    const query = mustGetQuery(queries, 'wide');

    type TableName = ReturnType<typeof query>['~']['$tableName'];

    expectTypeOf<'workspace'>().toExtend<TableName>();
    expectTypeOf<'user'>().toExtend<TableName>();
    expectTypeOf<'order'>().toExtend<TableName>();
  });

  test('multiple table queries maintain distinct types', async () => {
    const users = await zeroStress.run(zql.user);
    const products = await zeroStress.run(zql.product);
    const orders = await zeroStress.run(zql.order);

    type UserResult = (typeof users)[number];
    type ProductResult = (typeof products)[number];
    type OrderResult = (typeof orders)[number];

    expectTypeOf<UserResult>().toHaveProperty('userId');
    expectTypeOf<ProductResult>().toHaveProperty('productId');
    expectTypeOf<OrderResult>().toHaveProperty('orderId');
  });

  test('query chaining maintains type safety', async () => {
    const results = await zeroStress.run(
      zql.supportTicket
        .where('status', '=', 'open')
        .where('priority', '=', 'high')
        .orderBy('createdAt', 'desc')
        .limit(10),
    );

    type Result = (typeof results)[number];
    expectTypeOf<Result>().toHaveProperty('ticketId');
    expectTypeOf<Result>().toHaveProperty('status');
  });

  test('complex nested JSON types are preserved', () => {
    type UserRow = QueryResultType<typeof zql.user>[number];

    expectTypeOf<UserRow['metadata']['onboarding']>().toEqualTypeOf<{
      readonly completed: boolean;
      readonly step: number;
    }>();
  });

  test('enum types are preserved across different tables', () => {
    type Queries = typeof zql;

    type UserRow = QueryResultType<Queries['user']>[number];
    type WorkspaceRow = QueryResultType<Queries['workspace']>[number];
    type ProductRow = QueryResultType<Queries['product']>[number];
    type TicketRow = QueryResultType<Queries['supportTicket']>[number];

    expectTypeOf<UserRow['role']>().toEqualTypeOf<
      'owner' | 'admin' | 'member' | 'guest'
    >();
    expectTypeOf<UserRow['status']>().toEqualTypeOf<
      'active' | 'suspended' | 'deactivated'
    >();
    expectTypeOf<WorkspaceRow['plan']>().toEqualTypeOf<
      'free' | 'pro' | 'enterprise'
    >();
    expectTypeOf<WorkspaceRow['status']>().toEqualTypeOf<
      'active' | 'suspended' | 'trial'
    >();
    expectTypeOf<ProductRow['status']>().toEqualTypeOf<
      'active' | 'draft' | 'archived'
    >();
    expectTypeOf<TicketRow['status']>().toEqualTypeOf<
      'new' | 'open' | 'pending' | 'solved' | 'closed'
    >();
    expectTypeOf<TicketRow['priority']>().toEqualTypeOf<
      'low' | 'medium' | 'high' | 'urgent'
    >();
  });

  test('schema type can be inferred from Zero instance', () => {
    expectTypeOf<typeof zeroStress.schema>().toEqualTypeOf<
      typeof zeroStressSchema
    >();
  });

  test('query and mutation methods exist for all major tables', () => {
    expectTypeOf(zql.user['where']).toBeFunction();
    expectTypeOf(zql.workspace['where']).toBeFunction();
    expectTypeOf(zql.product['where']).toBeFunction();
    expectTypeOf(zql.order['where']).toBeFunction();
    expectTypeOf(zql.supportTicket['where']).toBeFunction();
    expectTypeOf(zql.emailCampaign['where']).toBeFunction();
    expectTypeOf(zql.inventoryAdjustment['where']).toBeFunction();
    expectTypeOf(zql.patient['where']).toBeFunction();
    expectTypeOf(zql.appointment['where']).toBeFunction();
    expectTypeOf(zql.invoice['where']).toBeFunction();
  });

  test('single-level relationship queries maintain type safety', async () => {
    // Query with one-to-many relationship
    const userQuery = zql.user.related('sessions');
    const users = await zeroStress.run(userQuery);

    type UserResult = (typeof users)[number];

    expectTypeOf<UserResult>().toHaveProperty('userId');
    expectTypeOf<UserResult>().toHaveProperty('email');
    expectTypeOf<UserResult['sessions'][number]>().toHaveProperty('sessionId');
    expectTypeOf<UserResult['sessions'][number]>().toHaveProperty('token');
  });

  test('relationship queries with filters maintain correct types', async () => {
    const results = await zeroStress.run(
      zql.user
        .where('role', '=', 'admin')
        .related('sessions', q => q.where('ipAddress', '=', '127.0.0.1'))
        .related('accounts', q =>
          q.where('provider', '=', 'google').orderBy('createdAt', 'desc'),
        ),
    );

    type Result = (typeof results)[number];

    expectTypeOf<Result['role']>().toEqualTypeOf<
      'owner' | 'admin' | 'member' | 'guest'
    >();
    expectTypeOf<Result['sessions'][number]['token']>().toBeString();
    expectTypeOf<Result['accounts'][number]['provider']>().toEqualTypeOf<
      'google' | 'github' | 'microsoft' | 'slack'
    >();
  });

  test('multiple independent relationship queries on same table', async () => {
    const query = zql.product
      .related('createdByUser')
      .related('updatedByUser')
      .related('workspace');

    const results = await zeroStress.run(query);

    type Result = (typeof results)[number];

    expectTypeOf<NonNullable<Result['createdByUser']>>().toHaveProperty(
      'userId',
    );
    expectTypeOf<NonNullable<Result['updatedByUser']>>().toHaveProperty(
      'userId',
    );
    expectTypeOf<NonNullable<Result['workspace']>>().toHaveProperty(
      'workspaceId',
    );
  });

  test('relationship queries preserve enum types through nesting', async () => {
    const results = await zeroStress.run(
      zql.product
        .where('status', '=', 'active')
        .related('createdByUser', q => q.where('role', '=', 'admin')),
    );

    type Result = (typeof results)[number];

    expectTypeOf<Result['status']>().toEqualTypeOf<
      'active' | 'draft' | 'archived'
    >();
    expectTypeOf<NonNullable<Result['createdByUser']>['role']>().toEqualTypeOf<
      'owner' | 'admin' | 'member' | 'guest'
    >();
  });

  test('relationship queries with limit and orderBy maintain types', async () => {
    const query = zql.user
      .where('emailVerified', '=', true)
      .related('sessions', q =>
        q
          .orderBy('lastActivityAt', 'desc')
          .limit(5)
          .where('expiresAt', '>', ''),
      )
      .related('accounts')
      .orderBy('createdAt', 'desc')
      .limit(10);

    const results = await zeroStress.run(query);

    type Result = (typeof results)[number];

    expectTypeOf<Result['sessions'][number]>().toHaveProperty('lastActivityAt');
    expectTypeOf<Result['accounts'][number]>().toHaveProperty('provider');
  });

  test('relationship types remain distinct across different parent queries', async () => {
    const products = await zeroStress.run(zql.product.related('workspace'));
    const orders = await zeroStress.run(zql.order.related('workspace'));

    type ProductWithWorkspace = (typeof products)[number];
    type OrderWithWorkspace = (typeof orders)[number];

    expectTypeOf<ProductWithWorkspace['workspace']>().toEqualTypeOf<
      OrderWithWorkspace['workspace']
    >();
  });

  test('relationship queries work across many table types simultaneously', async () => {
    const [users, products, orders, tickets, campaigns] = await Promise.all([
      zeroStress.run(zql.user.related('sessions').related('accounts')),
      zeroStress.run(zql.product.related('workspace').related('createdByUser')),
      zeroStress.run(zql.order.related('createdByUser')),
      zeroStress.run(zql.supportTicket.related('workspace')),
      zeroStress.run(zql.emailCampaign.related('workspace')),
    ]);

    type User = (typeof users)[number];
    type Product = (typeof products)[number];
    type Order = (typeof orders)[number];
    type Ticket = (typeof tickets)[number];
    type Campaign = (typeof campaigns)[number];

    expectTypeOf<User['sessions'][number]>().toHaveProperty('sessionId');
    expectTypeOf<NonNullable<Product['workspace']>>().toHaveProperty(
      'workspaceId',
    );
    expectTypeOf<NonNullable<Order['createdByUser']>>().toHaveProperty(
      'userId',
    );
    expectTypeOf<NonNullable<Ticket['workspace']>>().toHaveProperty(
      'workspaceId',
    );
    expectTypeOf<NonNullable<Campaign['workspace']>>().toHaveProperty(
      'workspaceId',
    );
  });

  test('deeply nested relationship chains', () => {
    type Result = (typeof queries.deep)['~']['$return'];

    type Creator = NonNullable<Result['createdByUser']>;
    type Workspace = NonNullable<
      Creator['workspaceMembers'][number]['workspace']
    >;
    type Department = NonNullable<Workspace['budgets'][number]['department']>;
    type Manager = NonNullable<
      NonNullable<
        NonNullable<Department['parentDepartment']>['headOfDepartment']
      >['manager']
    >;

    expectTypeOf<Result>().toHaveProperty('orderId');
    expectTypeOf<Creator>().toHaveProperty('userId');
    expectTypeOf<Workspace>().toHaveProperty('workspaceId');
    expectTypeOf<Department>().toHaveProperty('departmentId');
    expectTypeOf<Manager>().toHaveProperty('email');
  });

  test('wide parallel relationships maintain distinct types', () => {
    type Result = (typeof queries.wide)['~']['$return'];

    // Verify the root workspace type
    expectTypeOf<Result>().toHaveProperty('workspaceId');
    expectTypeOf<Result>().toHaveProperty('name');
    expectTypeOf<Result>().toHaveProperty('plan');

    // Sample relationships from different business domains
    expectTypeOf<Result['sessions'][number]>().toHaveProperty('sessionId');
    expectTypeOf<Result['accounts'][number]>().toHaveProperty('provider');
    expectTypeOf<Result['emailCampaigns'][number]>().toHaveProperty(
      'campaignId',
    );
    expectTypeOf<Result['subscribers'][number]>().toHaveProperty('email');
    expectTypeOf<Result['supportTickets'][number]>().toHaveProperty('status');
    expectTypeOf<Result['knowledgeBaseArticles'][number]>().toHaveProperty(
      'title',
    );
    expectTypeOf<Result['products'][number]>().toHaveProperty('productId');
    expectTypeOf<Result['orders'][number]>().toHaveProperty('orderId');
    expectTypeOf<Result['projects'][number]>().toHaveProperty('projectId');
    expectTypeOf<Result['tasks'][number]>().toHaveProperty('taskId');
    expectTypeOf<Result['sprints'][number]>().toHaveProperty('sprintId');
    expectTypeOf<Result['employees'][number]>().toHaveProperty('employeeId');
    expectTypeOf<Result['payrollRuns'][number]>().toHaveProperty('runId');
    expectTypeOf<Result['patients'][number]>().toHaveProperty('patientId');
    expectTypeOf<Result['appointments'][number]>().toHaveProperty(
      'appointmentId',
    );
  });
});
