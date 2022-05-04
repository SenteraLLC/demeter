from typing import Any, Tuple, Optional

import boto3
import os
import psycopg2
from psycopg2.extensions import connection as PGConnection
import getpass


def getEnv(name : str, default : Optional[str] = None):
  v = os.environ.get(name, default)
  if v is None:
    raise Exception(f"Environment variable for '{name}' not set")
  return v


def getS3Connection() -> Tuple[Any, str]:
  s3_role_arn = getEnv("S3_ROLE_ARN")
  bucket_name = getEnv("BUCKET_NAME")
  if bucket_name is None:
    raise Exception("Environment variable for 'S3_ROLE_ARN' not set")

  sts_client = boto3.client('sts')

  # Call the assume_role method of the STSConnection object and pass the role
  # ARN and a role session name.
  assumed_role_object=sts_client.assume_role(
    RoleArn=s3_role_arn,
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
  return s3_resource, bucket_name


def getPgConnection() -> PGConnection:
  host = getEnv("DemeterPGHOST", "localhost")
  user = getEnv("DemeterPGUSER", getpass.getuser())
  options = getEnv("DemeterPGOPTIONS", "")
  database = getEnv("DemeterPG", "postgres")
  return psycopg2.connect(host="localhost", options="-c search_path=test_mlops,public", database="postgres")


