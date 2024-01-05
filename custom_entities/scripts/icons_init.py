import json
import os
import sys

import boto3
import botocore

import core_utils.functions as functions
from core_utils.functions import initialize
from utils.exceptions.controller_exceptions import ControllerException


def initialize_functions():
    global UUID, TENANTS_BUCKET, S3_RESOURCE, S3_CLIENT
    initialize()
    UUID = functions.UUID
    TENANTS_BUCKET = os.environ.get("TENANTS_BUCKET")
    S3_RESOURCE = boto3.resource("s3")
    S3_CLIENT = boto3.client("s3")


def bucket_file_exist(path):
    try:
        S3_RESOURCE.Object(TENANTS_BUCKET, path).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # Something else has gone wrong.
            raise ControllerException(400, {"result": f"Error while bucket validation {str(e)}"})
    else:
        # The object does exist.
        return True


def create_bucket_file(file_path, key_path):
    try:
        S3_RESOURCE.Bucket(TENANTS_BUCKET).upload_file(file_path, key_path)
    except Exception as e:
        raise ControllerException(400, {"result": f"Error while bucket file creation {str(e)}"})


def create_default_assets():
    USER_CREATION_TEMPLATE_PATH = "./icons/IconObjects.json"
    S3_USER_CREATION_TEMPLATE_PATH = "assets_common/icon_objects/IconObjects.json"
    if not bucket_file_exist(S3_USER_CREATION_TEMPLATE_PATH):
        create_bucket_file(USER_CREATION_TEMPLATE_PATH, S3_USER_CREATION_TEMPLATE_PATH)


def lambda_handler(event, context):
    try:
        initialize_functions()
        create_default_assets()
        return {
            "statusCode": 200,
            "body": "Icon resources created",
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS,POST,PUT",
                "Access-Control-Allow-Headers": "Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers",
            },
        }
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ERROR_MSG = f"Execution failed: {repr(e)}. Line: {str(exc_tb.tb_lineno)}."
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": ERROR_MSG, "code": str(exc_type), "correlation_id": UUID}
            ),
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS,POST,PUT",
                "Access-Control-Allow-Headers": "Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers",
            },
        }
