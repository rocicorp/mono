export const capacityProvider = (
  prefix,
  {
    vpcId,
    privateSubnets,
    cluster,
  }: {
    vpcId: $util.Output<string>;
    privateSubnets: $util.Output<string>[];
    cluster: aws.ecs.Cluster;
  },
) => {
  const ec2Sg = new aws.ec2.SecurityGroup(`${prefix}Ec2Sg`, {
    name: `${prefix}_ec2_sg`,
    description: `${prefix} ec2 sg traffic rules`,
    vpcId,
    ingress: [
      {
        protocol: '-1', // All protocols
        fromPort: 0,
        toPort: 0,
        cidrBlocks: ['0.0.0.0/0'],
      },
    ],
    egress: [
      {
        protocol: '-1', // All protocols
        fromPort: 0,
        toPort: 0,
        cidrBlocks: ['0.0.0.0/0'], // Allow traffic to all IP addresses
      },
    ],
  });

  const assumeRole = aws.iam.getPolicyDocument({
    statements: [
      {
        effect: 'Allow',
        principals: [
          {
            type: 'Service',
            identifiers: ['ec2.amazonaws.com'],
          },
        ],
        actions: ['sts:AssumeRole'],
      },
    ],
  });

  const instanceRole = new aws.iam.Role(`${prefix}InstanceRole`, {
    name: `${prefix}ECSInstanceRole`,
    assumeRolePolicy: assumeRole.then(assumeRole => assumeRole.json),
  });

  // Attach the AmazonEC2ContainerServiceforEC2Role policy to the role
  new aws.iam.RolePolicyAttachment(`${prefix}InstanceRolePolicyAttachment`, {
    role: instanceRole.name,
    policyArn:
      'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role',
  });

  // Attach CloudWatch agent policy for Container Insights
  new aws.iam.RolePolicyAttachment(`${prefix}CloudWatchAgentPolicyAttachment`, {
    role: instanceRole.name,
    policyArn: 'arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy',
  });

  // Attach SSM policy to allow EC2 instance management
  new aws.iam.RolePolicyAttachment(`${prefix}SSMPolicyAttachment`, {
    role: instanceRole.name,
    policyArn: 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore',
  });

  // Attach custom policy for terminating instances in Auto Scaling Group
  const terminatePolicy = new aws.iam.Policy(
    `${prefix}InstanceTerminatePolicy`,
    {
      policy: JSON.stringify({
        // eslint-disable-next-line @typescript-eslint/naming-convention
        Version: '2012-10-17',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        Statement: [
          {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            Effect: 'Allow',
            // eslint-disable-next-line @typescript-eslint/naming-convention
            Action: [
              'autoscaling:TerminateInstanceInAutoScalingGroup',
              'ec2:TerminateInstances',
              'ec2:DescribeInstances',
            ],
            // eslint-disable-next-line @typescript-eslint/naming-convention
            Resource: '*',
          },
        ],
      }),
    },
  );

  new aws.iam.PolicyAttachment(`${prefix}InstanceTerminatePolicyAttachment`, {
    policyArn: terminatePolicy.arn,
    roles: [instanceRole.name],
  });

  const instanceProfile = new aws.iam.InstanceProfile(
    `${prefix}InstanceProfile`,
    {
      name: `${prefix}-instance-profile`,
      role: instanceRole.name,
    },
  );

  // Make the userData more explicit with an export path for ECS agent logs for debugging
  const userData = cluster.name.apply(
    name => `#!/bin/bash
            echo ECS_CLUSTER=${name} >> /etc/ecs/ecs.config
            echo ECS_LOGLEVEL=debug >> /etc/ecs/ecs.config
            echo ECS_AVAILABLE_LOGGING_DRIVERS='["json-file","awslogs"]' >> /etc/ecs/ecs.config
            systemctl restart ecs
            `,
  );

  const ec2Template = new aws.ec2.LaunchTemplate(
    `${prefix}LaunchTemplateECS`,
    {
      namePrefix: `${prefix}-instance-`,
      imageId: 'ami-00a929b66ed6e0de6',
      instanceType: 't2.small', // env
      vpcSecurityGroupIds: [ec2Sg.id],
      iamInstanceProfile: {
        arn: instanceProfile.arn,
      },
      userData: userData.apply(v => Buffer.from(v).toString('base64')),
    },
    {
      dependsOn: [cluster],
    },
  );

  const asg = new aws.autoscaling.Group(`${prefix}ASG`, {
    name: `${prefix}-asg`,
    forceDelete: true,
    maxSize: 4,
    minSize: 2,
    desiredCapacity: 2,
    launchTemplate: {
      name: ec2Template.name,
      version: '$Latest',
    },
    vpcZoneIdentifiers: privateSubnets,
    defaultCooldown: 20,
    // Add tags to help with ECS cluster discovery
    tags: [
      {
        key: 'AmazonECSManaged',
        value: '',
        propagateAtLaunch: true,
      },
    ],
  });

  const capacityProvider = new aws.ecs.CapacityProvider(
    `${prefix}CapacityProvider`,
    {
      name: `${prefix}-provider`,
      autoScalingGroupProvider: {
        autoScalingGroupArn: asg.arn,
        managedScaling: {
          minimumScalingStepSize: 1,
          maximumScalingStepSize: 2,
          status: 'ENABLED',
          targetCapacity: 30,
        },
        managedTerminationProtection: 'DISABLED', // To allow scale-in during development
      },
    },
    {
      dependsOn: [asg, cluster],
    },
  );

  const clusterCapacityProviders = new aws.ecs.ClusterCapacityProviders(
    `${prefix}ClusterCapacityProviderAssoc`,
    {
      clusterName: cluster.name,
      capacityProviders: [capacityProvider.name],
      defaultCapacityProviderStrategies: [
        {
          capacityProvider: capacityProvider.name,
          weight: 1,
          base: 1,
        },
      ],
    },
    {
      dependsOn: [capacityProvider, cluster],
    },
  );

  return {
    ec2Sg,
    capacityProvider,
    clusterCapacityProviders,
  };
};
