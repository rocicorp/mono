export const createService = (
  prefix,
  {
    vpcId,
    privateSubnets,
    cluster,
    capacityProvider,
    targetGroup,
    albSecurityGroup,
    commonEnv,
    port, // Default to the view-syncer port, but allow override
    alb, // Add ALB parameter to access its DNS name
  }: {
    vpcId: $util.Output<string>;
    privateSubnets: $util.Output<string>[];
    cluster: aws.ecs.Cluster;
    capacityProvider: aws.ecs.CapacityProvider;
    albSecurityGroup: aws.ec2.SecurityGroup;
    targetGroup: aws.lb.TargetGroup;
    commonEnv: Record<string, string | $util.Output<string>>;
    port: number;
    alb: aws.lb.LoadBalancer;
  },
) => {
  const taskDefExecutionRole = new aws.iam.Role(`${prefix}TaskDefExecRole`, {
    assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Service: 'ecs-tasks.amazonaws.com',
    }),
    managedPolicyArns: [
      'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy',
    ],
  });

  // Define the container name consistently
  const containerName = 'zero-cache-image';

  // Create the log group explicitly to ensure it exists
  const logGroupName = `/ecs/${prefix}`;
  const logGroup = new aws.cloudwatch.LogGroup(`${prefix}LogGroup`, {
    name: logGroupName,
    retentionInDays: 30, // Set an appropriate retention period
  });

  // Use Pulumi's Output.all to properly unwrap all Output values
  return $util
    .all(
      // Collect all potential Output values from commonEnv
      Object.entries(commonEnv).map(([_name, value]) =>
        typeof value === 'string' ? value : value,
      ),
    )
    .apply(resolvedValues => {
      // Create resolved environment variables
      const resolvedEnv = Object.fromEntries(
        Object.entries(commonEnv).map(([name, _value], i) => [
          name,
          resolvedValues[i],
        ]),
      );

      const containerDefinition = {
        name: containerName,
        image: resolvedEnv.ZERO_IMAGE_URL,
        cpu: 800,
        memory: 800,
        essential: true,
        portMappings: [
          {
            containerPort: port,
            hostPort: port,
            protocol: 'tcp',
          },
        ],
        // Add health check configuration with correct port
        healthCheck: {
          command: [
            `CMD-SHELL`,
            `curl -f http://localhost:${port}/keepalive || exit 1`,
          ],
          interval: 5,
          timeout: 3,
          retries: 3,
          startPeriod: 300,
        },
        environment: Object.entries(resolvedEnv).map(([name, value]) => ({
          name,
          value,
        })),
        logConfiguration: {
          logDriver: 'awslogs',
          options: {
            'awslogs-group': logGroupName,
            'awslogs-region': process.env.AWS_REGION || 'us-east-1',
            'awslogs-stream-prefix': 'ecs',
          },
        },
      };

      const taskDef = new aws.ecs.TaskDefinition(`${prefix}TD`, {
        family: `${prefix.toLowerCase()}-web-task`,
        networkMode: 'awsvpc',
        requiresCompatibilities: ['EC2'],
        runtimePlatform: {
          cpuArchitecture: 'X86_64',
        },
        executionRoleArn: taskDefExecutionRole.arn,
        containerDefinitions: JSON.stringify([containerDefinition]),
      });

      const serviceSG = new aws.ec2.SecurityGroup(`${prefix}ServiceSg`, {
        description: `${prefix} service sg`,
        vpcId,
        ingress: [
          {
            protocol: 'tcp',
            fromPort: 80,
            toPort: 80,
            securityGroups: [albSecurityGroup.id],
          },
          {
            protocol: 'tcp',
            fromPort: 443,
            toPort: 443,
            securityGroups: [albSecurityGroup.id],
          },
          {
            protocol: 'tcp',
            fromPort: port,
            toPort: port,
            securityGroups: [albSecurityGroup.id],
          },
        ],
        egress: [
          {
            protocol: '-1', // All protocols
            fromPort: 0,
            toPort: 0,
            cidrBlocks: ['0.0.0.0/0'], // Allow outbound internet access
          },
        ],
      });

      const service = new aws.ecs.Service(
        `${prefix}Service`,
        {
          cluster: cluster.arn,
          taskDefinition: taskDef.arn,
          desiredCount: 1,
          networkConfiguration: {
            subnets: privateSubnets,
            securityGroups: [serviceSG.id],
            assignPublicIp: false,
          },
          loadBalancers: [
            {
              targetGroupArn: targetGroup.arn,
              containerName,
              containerPort: port, // Use the correct port
            },
          ],
          capacityProviderStrategies: [
            {
              capacityProvider: capacityProvider.name,
              weight: 1,
            },
          ],
          orderedPlacementStrategies: [
            {
              type: 'binpack',
              field: 'memory',
            },
          ],
          healthCheckGracePeriodSeconds: 600, // 10 minutes grace period
          deploymentMaximumPercent: 200,
          deploymentMinimumHealthyPercent: 50,
          deploymentController: {
            type: 'ECS',
          },
          deploymentCircuitBreaker: {
            enable: true,
            rollback: true,
          },
          forceNewDeployment: true,
        },
        {
          dependsOn: [
            capacityProvider,
            cluster,
            taskDef,
            logGroup, // Add the log group as a dependency
          ],
        },
      );

      // Create the service URL using the ALB's DNS name
      const serviceUrl = alb.dnsName.apply(dnsName => {
        // Use HTTPS if a domain and cert are configured, otherwise HTTP
        const protocol =
          process.env.DOMAIN_NAME && process.env.DOMAIN_CERT ? 'https' : 'http';
        return `${protocol}://${dnsName}`;
      });

      // Return both the service and the URL
      return {
        service,
        serviceUrl,
      };
    });
};
