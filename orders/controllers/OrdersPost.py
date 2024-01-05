import base64
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

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
    webhook_dispatch,
)


def initialize_functions():
    global UUID, CURRENT_DATETIME
    initialize()
    UUID = functions.UUID
    CURRENT_DATETIME = functions.CURRENT_DATETIME


def is_contract_available(contract_id, start_date, end_date, time_zone):
    sql_is_contract_active = (
        f"SELECT is_active FROM contracts_master where contract_id = '{contract_id}'"
    )
    is_active_result = rds_execute_statement(sql_is_contract_active)["records"][0][0][
        "booleanValue"
    ]
    sql_start_date = f"SELECT start_date FROM contracts_master where contract_id = '{contract_id}'"
    start_date_result = rds_execute_statement(sql_start_date)["records"][0][0]["stringValue"]
    sql_end_date = f"SELECT end_date FROM contracts_master where contract_id = '{contract_id}'"
    end_date_result = rds_execute_statement(sql_end_date)["records"][0][0]["stringValue"]

    # Get today's date in the user time_zone
    sql = f"""SELECT  utc_offset FROM pg_timezone_names WHERE name = '{time_zone}'"""
    rds_response = rds_execute_statement(sql)
    time_zone_data = deserialize_rds_response(rds_response)[0]
    # Get offset
    offset_hours = int(
        time_zone_data["utc_offset"].split("days")[1].split("hours")[0].replace(" ", "")
    )
    offset_minutes = int(
        time_zone_data["utc_offset"].split("hours")[1].split("mins")[0].replace(" ", "")
    )

    # Create delta and create the timezone based on that delta
    delta = timedelta(hours=offset_hours, minutes=offset_minutes)
    today = datetime.now(timezone(delta)).date()

    start_date_result = datetime.strptime(start_date_result, "%Y-%m-%d").date()
    end_date_result = datetime.strptime(end_date_result, "%Y-%m-%d").date()

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if not is_active_result:
        return False, "ordersCreation.ContractIsNotActive"
    if start_date_result > start_date:
        return False, "ordersCreation.ContractStartDateIsGreaterThanOrderStartDate"
    if end_date_result < end_date:
        return False, "ordersCreation.ContractEndDateIsLessThanOrderEndDate"
    if today > start_date:
        return False, "ordersCreation.OrderStartDateIsLessThanCurrentDate"
    if start_date > end_date:
        return False, "ordersCreation.EndDateIsLessThanStartDate"

    return True, "Success"


def is_name_available(order_name):
    sql_is_name_available = f"SELECT COUNT(*) FROM orders_master WHERE order_name = '{order_name}'"
    response = rds_execute_statement(sql_is_name_available)["records"][0][0]["longValue"]
    print(response)
    if response == 0:
        return True
    else:
        return False


def create_order(
    contract_id=None,
    order_name=None,
    start_date=None,
    end_date=None,
    user_account_id=None,
    user_id=None,
    account_id=None,
):
    # replace ' with '' to not have error in the query
    if order_name is not None:
        order_name = str(order_name).replace(r"'", r"''")

    sql = f"SELECT time_zone FROM users_master WHERE cognito_user_id = '{user_id}'"
    response = rds_execute_statement(sql)["records"]
    if len(response):
        time_zone = response[0][0]["stringValue"]
    else:
        time_zone = "UTC"

    valid_contract, code = is_contract_available(contract_id, start_date, end_date, time_zone)
    if valid_contract:
        sql_create_order = "INSERT INTO orders_master (account_id, order_name, status, contract_id, start_date, end_date, created_by, updated_by) "
        sql_create_order += f"VALUES ('{user_account_id}','{order_name}','draft','{contract_id}','{start_date}','{end_date}','{user_id}', '{user_id}') RETURNING order_id"
        result = rds_execute_statement(sql_create_order)
        print(sql_create_order)
        order_id = deserialize_rds_response(result)[0]["order_id"]

        response = (
            200,
            {
                "correlation_id": UUID,
                "code": "Order created",
                "order_id": order_id,
                "order_name": order_name,
            },
        )
    else:
        response = (400, {"correlation_id": UUID, "code": code})

    return response


@webhook_dispatch(object_name="orders_master", action="create")
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
        event_body = json.loads(event["body"])
        account_id = get_account_id(user_id)
        if account_id is None:
            user_account_id = event_body["account_id"]
            event_body.pop("account_id")
        else:
            user_account_id = account_id
        # GET BODY

        if check_tenant_level_permissions(tenant_id, "account", "order", "general"):
            # Check user level permissions
            if check_user_level_permissions(
                tenant_id, user_id, "account", "order", "general", "can_create"
            ):
                create_transaction()
                result = create_order(
                    **event_body, user_account_id=user_account_id, user_id=user_id
                )
                confirm_transaction()
                response = result

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
