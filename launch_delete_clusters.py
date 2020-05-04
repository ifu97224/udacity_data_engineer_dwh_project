import argparse
import configparser
import boto3
from botocore.exceptions import ClientError
import json

def create_clients(KEY, SECRET):
    """Function to create iam, redshift and ec2 clients
    
    Parameters
    ----------
    KEY : str
        AWS key defined in the config
    SECRET : str
        AWS secret defined in the config

    Returns
    -------
    iam : boto3.client
        iam client for creating iam roles
    redshift : boto3.client
        redshift client for creating redshift clusters
    ec2 : boto3.client
        ec2 client
    """
    
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    return iam, redshift, ec2

def create_iam_policy(DWH_IAM_ROLE_NAME):
    """Function to create an iam policy to allow Redshift read only access to S3 buckets
    
    Parameters
    ----------
    DWH_IAM_ROLE_NAME : str
        name for the iam role
    Returns
    -------
    roleArn : str
        string containing the role ARN
    """
    
    # Create an iam role that allows Redshift to access S3 buckets with read only access
    
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)


    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    return roleArn

def create_redshift_cluster(DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                           DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):

    """Function to create a Redshift cluster
    
    Parameters
    ----------
    DWH_CLUSTER_TYPE : str
        string containing the DWH cluster type defined in the config
    DWH_NODE_TYPE : str
        string containing the DWH node type defined in the config
    DWH_NUM_NODES : str
        string containing the number of nodes to create defined in the config
    DWH_DB : str
        string containing the database name defined in the config
    DWH_CLUSTER_IDENTIFIER : str
        string containing the identifier for the cluster as defined in the config
    DWH_DB_USER : str
        string containing the master user name as defined in the config
    DWH_DB_PASSWORD : str
        password for the cluster as defined in the config
   
    """
    
    # Create a Redshift cluster
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
    )
    except Exception as e:
        print(e)

def open_tcp_port():
    """Function to open a TCP port to access the cluster endpoint"""
    
    # Open an incoming tcp port to access the cluster endpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
if __name__ == "__main__":

    # Get the arguments identifying if clusters should be created or deleted
    parser = argparse.ArgumentParser()
    parser.add_argument("--create_or_delete", 
                        type=str, 
                        default='create', 
                        help='takes value "create" if a cluster is to be created or "delete" if a cluster is to be deleted'
                       )
    
    args, _ = parser.parse_known_args()
    
    # Get key, secret and cluster details from the config
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    # Create a cluster if requested
    if args.create_or_delete == 'create':
        # Create iam, EC2 and Redshift clients
        iam, redshift, ec2 = create_clients(KEY, SECRET)

        # Create iam policy to allow (read only) access to S3
        roleArn = create_iam_policy(DWH_IAM_ROLE_NAME)
        print(roleArn)

        # Create a Redshift cluster
        create_redshift_cluster(DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                               DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)

        print('Creating cluster...')
        while True:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            myClusterProps['ClusterStatus'] != 'available'
            if myClusterProps['ClusterStatus'] == 'available':
                break

        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

        # Open an incoming tcp port to access the cluster endpoint
        open_tcp_port()
        
    # Delete a cluster
    if args.create_or_delete == 'delete':
        
        # Create iam, EC2 and Redshift clients
        iam, redshift, ec2 = create_clients(KEY, SECRET)
        
        # delete the cluster
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        
        # detatch iam role
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)