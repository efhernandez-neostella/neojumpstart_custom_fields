import base64
import json
import os
import sys
import uuid
from datetime import datetime, timedelta

import boto3

import core_utils.functions as functions

# import Values
# import core_utils.functions as functions
from core_utils.functions import (
    APPKEY_SECRET_ARN,
    COGNITO_CLIENT,
    CORALOGIX_KEY,
    DATABASE_NAME,
    DB_CLUSTER_ARN,
    DB_CREDENTIALS_SECRETS_STORE_ARN,
    RDS_CLIENT,
    REGION_NAME,
    RESOURCE_METHOD,
    SERVICE_NAME,
    check_api_keys,
    check_tenant_level_permissions,
    check_tenant_limit,
    check_user_level_permissions,
    confirm_transaction,
    create_transaction,
    decode_key,
    delete_transaction,
    deserialize_rds_response,
    get_account_id,
    get_pool_id,
    get_secret,
    get_tenant_id,
    initialize,
    invoke_requested_version,
    rds_execute_statement,
    send_to_coralogix,
    throttling_check,
    wait_for_threads,
)


def initialize_functions():
    global UUID, CURRENT_DATETIME
    initialize()
    UUID = functions.UUID
    CURRENT_DATETIME = functions.CURRENT_DATETIME


def get_cart_order(order_products_id=None):
    sql_get_cart_order = f"SELECT * FROM order_products INNER JOIN products_master ON order_products.product_id = products_master.product_id INNER JOIN orders_master ON order_products.order_id = orders_master.order_id INNER JOIN accounts_master ON orders_master.account_id = accounts_master.account_id WHERE order_products_id = '{order_products_id}';"
    records = deserialize_rds_response(rds_execute_statement(sql_get_cart_order))[0]
    currency_id = records["currency_id"]
    sql_get_currency_name = (
        f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
    )
    currency = deserialize_rds_response(rds_execute_statement(sql_get_currency_name))[0][
        "currency_name"
    ]
    records["currency"] = currency
    records.pop("currency_id")
    return records


def lambda_handler(event, context):
    try:
        # Invoke previous version if requested
        LAMBDA_ARN = context.invoked_function_arn
        if "version" in event["headers"]:
            response = invoke_requested_version(LAMBDA_ARN, event)
            return response
        # INITIATE GLOBAL DATABASE CREDENTIALS
        initialize_functions()
        # Send to Coralogix the request data
        send_to_coralogix(
            CORALOGIX_KEY,
            {"correlation_id": UUID, "Event Received": event},
            SERVICE_NAME,
            RESOURCE_METHOD,
            3,
        )
        user_id, tenant_id = check_api_keys(event)

        # GET BODY
        query_parameters = event["queryStringParameters"]
        if check_tenant_level_permissions(tenant_id, "account", "order", "general"):
            # Check user level permissions
            if check_user_level_permissions(
                tenant_id, user_id, "account", "order", "order_list", "can_read"
            ):
                # here is the code to get the orders of an user
                records = get_cart_order(**query_parameters)
                response = (200, {"correlation_id": UUID, "records": records})

            else:
                response = (
                    403,
                    {
                        "correlation_id": UUID,
                        "code": "role_permissions.UserDoesNotHaveAccessToThisFeature",
                    },
                )
        else:
            response = (
                403,
                {
                    "correlation_id": UUID,
                    "code": "tenant_permissions.TenantDoesNotHaveAccessToThisFeature",
                },
            )

        EXECUTION_TIME = str(datetime.now() - CURRENT_DATETIME)
        send_to_coralogix(
            CORALOGIX_KEY,
            {"correlation_id": UUID, "Execution time": EXECUTION_TIME, "response": response[1]},
            SERVICE_NAME,
            RESOURCE_METHOD,
            3,
        )
        wait_for_threads()
        return {
            "statusCode": response[0],
            "body": json.dumps(response[1]),
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS,POST,PUT,DELETE",
                "Access-Control-Allow-Headers": "Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers",
            },
        }
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        delete_transaction()
        ERROR_MSG = f"Execution failed: {repr(e)}. Line: {str(exc_tb.tb_lineno)}."
        EXECUTION_TIME = str(datetime.now() - CURRENT_DATETIME)
        send_to_coralogix(
            CORALOGIX_KEY,
            {
                "correlation_id": UUID,
                "Status": "Failure",
                "Execution time": EXECUTION_TIME,
                "Error message": ERROR_MSG,
            },
            SERVICE_NAME,
            RESOURCE_METHOD,
            5,
        )
        wait_for_threads()
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": ERROR_MSG, "code": str(exc_type), "correlation_id": UUID}
            ),
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS,POST,PUT,DELETE",
                "Access-Control-Allow-Headers": "Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers",
            },
        }
