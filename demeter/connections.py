from typing import Any, Tuple, Optional, Callable

import boto3
import os
import getpass

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConnection
from psycopg2.extensions import register_adapter, adapt
from collections import OrderedDict


def getEnv(name : str, default : Optional[str] = None) -> str:
  v = os.environ.get(name, default)
  if v is None:
    raise Exception(f"Environment variable for '{name}' not set")
  return v

def getS3Connection() -> Tuple[Any, str]:
  s3_role_arn = getEnv("DEMETER_S3_ROLE_ARN")
  bucket_name = getEnv("DEMETER_BUCKET_NAME")
  if bucket_name is None:
    raise Exception("Environment variable for 'DEMETER_S3_ROLE_ARN' not set")

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
  # TODO: Move this closer to wherever it is used
  register_adapter(set, lambda s : adapt(list(s))) # type: ignore

  host = getEnv("DEMETER_PG_HOST", "localhost")
  user = getEnv("DEMETER_PG_USER", getpass.getuser())
  options = getEnv("DEMETER_PG_OPTIONS", "")
  database = getEnv("DEMETER_PG_DATABASE", "postgres")
  return psycopg2.connect(host=host, options=options, database=database, user=user, cursor_factory=psycopg2.extras.NamedTupleCursor)


