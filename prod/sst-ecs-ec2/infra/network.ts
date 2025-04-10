export const networkConfig = (namePrefix) => {

  const vpc = new aws.ec2.Vpc(`${namePrefix}VPC`, {
    cidrBlock: "10.0.0.0/16",
    enableDnsSupport: true,
    enableDnsHostnames: true,
  });

  // Create an Internet Gateway and attach it to the VPC
  const internetGateway = new aws.ec2.InternetGateway(`${namePrefix}IGW`, {
    vpcId: vpc.id,
  });

  // Create a public route table for public subnets
  const publicRouteTable = new aws.ec2.RouteTable(`${namePrefix}PublicRouteTable`, {
    vpcId: vpc.id,
    routes: [
      {
        cidrBlock: "0.0.0.0/0",
        gatewayId: internetGateway.id,
      },
    ],
  });

  // Create a public subnet for the NAT instance
  const publicSubnetNat = new aws.ec2.Subnet(`${namePrefix}PublicSubnetNat`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.64.0/20",
    mapPublicIpOnLaunch: true,  // Changed to true to allow public IP assignment
    availabilityZone: "us-east-1a",
  });

  // Create public subnets for ALB
  const albPublicSubnet1 = new aws.ec2.Subnet(`${namePrefix}ALBPublicSubnet1`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.0.0/20",
    mapPublicIpOnLaunch: true,  // Changed to true for ALB to access internet
    availabilityZone: "us-east-1a",
  });

  const albPublicSubnet2 = new aws.ec2.Subnet(`${namePrefix}ALBPublicSubnet2`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.80.0/20",
    mapPublicIpOnLaunch: true,  // Changed to true for ALB to access internet
    availabilityZone: "us-east-1b",
  });

  // Associate the public route table with the public subnets
  new aws.ec2.RouteTableAssociation(`${namePrefix}PublicNatRouteTableAssoc`, {
    subnetId: publicSubnetNat.id,
    routeTableId: publicRouteTable.id,
  });

  new aws.ec2.RouteTableAssociation(`${namePrefix}PublicAlb1RouteTableAssoc`, {
    subnetId: albPublicSubnet1.id,
    routeTableId: publicRouteTable.id,
  });

  new aws.ec2.RouteTableAssociation(`${namePrefix}PublicAlb2RouteTableAssoc`, {
    subnetId: albPublicSubnet2.id,
    routeTableId: publicRouteTable.id,
  });

  // Create private subnets for EC2 instance
  const privateSub1 = new aws.ec2.Subnet(`${namePrefix}PrivateSubnet1`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.32.0/20",
    availabilityZone: "us-east-1a",
  });

  const privateSub2 = new aws.ec2.Subnet(`${namePrefix}PrivateSubnet2`, {
    vpcId: vpc.id,
    cidrBlock: "10.0.96.0/20",
    availabilityZone: "us-east-1b",
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
  
  //fckNat
  const amiId = aws.ec2.getAmi({
    filters: [
      {
        name: "name",
        // The AMI has the SSM agent pre-installed
        values: ["fck-nat-al2023-*"],
      },
      {
        name: "architecture",
        values: ["arm64"],
      },
    ],
    mostRecent: true,
    owners: ["568608671756"],
}, { async: true }).then(ami => ami.id)

  
  // Create a NAT Instance
  const natInstance = new aws.ec2.Instance(`${namePrefix}NatInstance`, {
    instanceType: "t4g.nano",
    ami: amiId, // Amazon Linux 2 AMI
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
