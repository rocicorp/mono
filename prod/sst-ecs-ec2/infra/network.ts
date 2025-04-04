export const networkConfig = (namePrefix) => {

  const vpc = new aws.ec2.DefaultVpc(`${namePrefix}VPC`, {
    enableDnsSupport: true,
    enableDnsHostnames: true,
  });

  // Create a public subnet for the NAT instance
  const publicSubnetNat = new aws.ec2.Subnet(`${namePrefix}PublicSubnetNat`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.0.0/16",
    mapPublicIpOnLaunch: false,
  });

  // Create a public subnets for ALB
  const albPublicSubnet1 = new aws.ec2.Subnet(`${namePrefix}ALBPublicSubnet1`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.0.0/20",
    mapPublicIpOnLaunch: false,
  });

  const albPublicSubnet2 = new aws.ec2.Subnet(`${namePrefix}ALBPublicSubnet2`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.16.0/20",
    mapPublicIpOnLaunch: false,
  });

  // Create private subnets for EC2 instance
  const privateSub1 = new aws.ec2.Subnet(`${namePrefix}PrivateSubnet1`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.32.0/20",
  });

  const privateSub2 = new aws.ec2.Subnet(`${namePrefix}PrivateSubnet2`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.48.0/20",
  });

  // Create a Security Group for the NAT instance
  const natSecurityGroup = new aws.ec2.SecurityGroup(`${namePrefix}NATSecurityGroup`, {
    vpcId: vpc.id,
    ingress: [
      {
        protocol: "-1", // All traffic
        fromPort: 0,
        toPort: 0,
        cidrBlocks: ["0.0.0.0/0"],
      },
    ],
    egress: [
      {
        protocol: "-1", // All traffic
        fromPort: 0,
        toPort: 0,
        cidrBlocks: ["0.0.0.0/0"],
      },
    ],
  });

  // Create a NAT Instance
  const natInstance = new aws.ec2.Instance("SimpleNatInstance22", {
    instanceType: "t2.nano",
    ami: "ami-0695b862c585a60a7", // Amazon Linux 2 AMI
    subnetId: publicSubnetNat.id,
    associatePublicIpAddress: true,
    vpcSecurityGroupIds: [natSecurityGroup.id],
    sourceDestCheck: false,
    tags: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Name: `${namePrefix}-nat-instance`,
    },
  });

  // Create a Route Table for the private subnet
  const privateRouteTable = new aws.ec2.RouteTable(`${namePrefix}PrivateRouteTable`, {
    vpcId: vpc.id,
    routes: [
      {
        cidrBlock: "0.0.0.0/0",
        networkInterfaceId: natInstance.primaryNetworkInterfaceId,
      },
    ],
  });

  // Associate the route table with the private subnet 1
  new aws.ec2.RouteTableAssociation(`${namePrefix}PrivateRouteTableAssoc1`, {
    subnetId: privateSub1.id,
    routeTableId: privateRouteTable.id,
  });

  // Associate the route table with the private subnet 2
  new aws.ec2.RouteTableAssociation(`${namePrefix}PrivateRouteTableAssoc2`, {
    subnetId: privateSub2.id,
    routeTableId: privateRouteTable.id,
  });

  return {
    vpcId: vpc.id,
    privateSubnets: [privateSub1.id, privateSub2.id],
    albPublicSunets: [albPublicSubnet1.id, albPublicSubnet2.id],
    nat: natInstance.id,
  };
};
