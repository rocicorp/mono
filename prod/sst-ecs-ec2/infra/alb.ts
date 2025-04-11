export const createAlb = (
  prefix,
  {
    vpcId,
    publicSubnets,
    privateSubnets,
    domainName,
    domainCertArn,
  }: {
    vpcId: $util.Output<string>;
    publicSubnets: $util.Output<string>[];
    privateSubnets?: $util.Output<string>[];
    domainName?: string;
    domainCertArn?: string;
  },
) => {
  // Create a Security Group for the ALB
  const albSecurityGroup = new aws.ec2.SecurityGroup(
    `${prefix}-ALBSecurityGroup`,
    {
      vpcId,
      description: 'Allow HTTP/HTTPS inbound traffic',
      ingress: [
        {
          protocol: 'tcp',
          fromPort: 80,
          toPort: 80,
          cidrBlocks: ['0.0.0.0/0'],
        },
        {
          protocol: 'tcp',
          fromPort: 443,
          toPort: 443,
          cidrBlocks: ['0.0.0.0/0'],
        },
      ],
      egress: [
        {
          protocol: '-1',
          fromPort: 0,
          toPort: 0,
          cidrBlocks: ['0.0.0.0/0'],
        },
      ],
    },
  );

  // Create the ALB
  const alb = new aws.lb.LoadBalancer(`${prefix}-ALB`, {
    internal: false,
    securityGroups: [albSecurityGroup.id],
    subnets: publicSubnets,
    loadBalancerType: 'application',
    idleTimeout: 3600,
    enableCrossZoneLoadBalancing: true,
  });

  // Create Target Groups with health check configurations that mirror SST config
  const targetGroup = new aws.lb.TargetGroup(`${prefix}-TargetGroup`, {
    port: 80,
    protocol: 'HTTP',
    vpcId,
    targetType: 'ip',
    healthCheck: {
      enabled: true,
      path: '/keepalive',
      port: '4848',
      protocol: 'HTTP',
      interval: 5,
      healthyThreshold: 2,
      unhealthyThreshold: 2,
      timeout: 3,
    },
    deregistrationDelay: 1,
    stickiness: {
      enabled: true,
      type: 'lb_cookie',
      cookieDuration: 120,
    },
    loadBalancingAlgorithmType: 'least_outstanding_requests',
  });

  // Create HTTP Listener (port 80)
  const httpListener = new aws.lb.Listener(`${prefix}-HTTPListener`, {
    loadBalancerArn: alb.arn,
    port: 80,
    protocol: 'HTTP',
    defaultActions:
      domainName && domainCertArn
        ? [
            {
              type: 'redirect',
              redirect: {
                port: '443',
                protocol: 'HTTPS',
                statusCode: 'HTTP_301',
              },
            },
          ]
        : [
            {
              type: 'forward',
              targetGroupArn: targetGroup.arn,
            },
          ],
  });

  // Create HTTPS Listener (port 443) if domain and cert are provided
  let httpsListener;
  if (domainName && domainCertArn) {
    httpsListener = new aws.lb.Listener(`${prefix}-HTTPSListener`, {
      loadBalancerArn: alb.arn,
      port: 443,
      protocol: 'HTTPS',
      sslPolicy: 'ELBSecurityPolicy-2016-08',
      certificateArn: domainCertArn,
      defaultActions: [
        {
          type: 'forward',
          targetGroupArn: targetGroup.arn,
        },
      ],
    });
  }

  // Create an internal load balancer if private subnets are provided
  let internalAlbSecurityGroup, internalTargetGroup, internalHttpListener;
  let internalAlb: aws.lb.LoadBalancer | undefined;

  if (privateSubnets && privateSubnets.length > 0) {
    // Create a Security Group for the internal ALB
    internalAlbSecurityGroup = new aws.ec2.SecurityGroup(
      `${prefix}-IALBSecurityGroup`,
      {
        vpcId,
        description: 'Allow VPC HTTP inbound traffic',
        ingress: [
          {
            protocol: 'tcp',
            fromPort: 80,
            toPort: 80,
            cidrBlocks: ['0.0.0.0/0'], // For simplicity; consider restricting to VPC CIDR in production
          },
        ],
        egress: [
          {
            protocol: '-1', // All traffic
            fromPort: 0,
            toPort: 0,
            cidrBlocks: ['0.0.0.0/0'],
          },
        ],
      },
    );

    // Create the internal ALB
    internalAlb = new aws.lb.LoadBalancer(`${prefix}-IALB`, {
      internal: true, // This is the key difference for an internal load balancer
      securityGroups: [internalAlbSecurityGroup.id],
      subnets: privateSubnets,
      loadBalancerType: 'application',
      idleTimeout: 3600,
      enableCrossZoneLoadBalancing: true,
    });

    // Create internal target group for the replication manager
    internalTargetGroup = new aws.lb.TargetGroup(`${prefix}-ITG`, {
      port: 4849,
      protocol: 'HTTP',
      vpcId,
      targetType: 'ip',
      healthCheck: {
        enabled: true,
        path: '/keepalive',
        port: '4849',
        protocol: 'HTTP',
        interval: 5,
        healthyThreshold: 2,
        unhealthyThreshold: 2,
        timeout: 3,
      },
      deregistrationDelay: 1,
    });

    // Create HTTP Listener for internal ALB
    internalHttpListener = new aws.lb.Listener(`${prefix}-IHTTPListener`, {
      loadBalancerArn: internalAlb.arn,
      port: 80,
      protocol: 'HTTP',
      defaultActions: [
        {
          type: 'forward',
          targetGroupArn: internalTargetGroup.arn,
        },
      ],
    });
  }

  return {
    alb,
    albSecurityGroup,
    targetGroup,
    httpListener,
    httpsListener,

    // Internal ALB resources (may be undefined if privateSubnets not provided)
    internalAlb,
    internalAlbSecurityGroup,
    internalTargetGroup,
    internalHttpListener,
  };
};
