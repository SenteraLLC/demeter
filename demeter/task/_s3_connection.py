import os
from typing import Any, Callable, Tuple

import boto3

from ..db._postgres import getEnv


def getS3Connection() -> Tuple[Any, str]:
    s3_role_arn = getEnv("DEMETER_S3_ROLE_ARN")
    bucket_name = getEnv("DEMETER_BUCKET_NAME")
    if bucket_name is None:
        raise Exception("Environment variable for 'DEMETER_S3_ROLE_ARN' not set")

    sts_client = boto3.client("sts")

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object = sts_client.assume_role(
        RoleArn=s3_role_arn,
        # TODO: Auto-generate session name from some metadata
        RoleSessionName="AssumeRoleSession1",
    )

    credentials = assumed_role_object["Credentials"]

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    return s3_resource, bucket_name
