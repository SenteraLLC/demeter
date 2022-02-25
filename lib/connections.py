from psycopg2.extensions import connection as PGConnection
import boto3

S3Connection = boto3.resources.factory.ServiceResource

def getS3Connection(role_arn : str) -> S3Connection:
  sts_client = boto3.client('sts')

  # Call the assume_role method of the STSConnection object and pass the role
  # ARN and a role session name.
  assumed_role_object=sts_client.assume_role(
    RoleArn=role_arn,
    # TODO: Auto-generate session name from some metadata
    RoleSessionName="AssumeRoleSession1"
  )

  credentials=assumed_role_object['Credentials']

  s3_resource=boto3.resource(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
  )
  return s3_resource

