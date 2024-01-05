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


def get_contracts(
    user_account_id=None,
    tenant_id=None,
    contract_id=None,
    contract_name=None,
    account_id=None,
    start_date=None,
    end_date=None,
    is_active=None,
    limit=None,
    page=None,
):
    """
    Get all contracts filtered by values
    tenant_id = uuid. Required
    contract_id = uuid.
    is_active = enabled/disabled
    contract_name = str.
    account_id = uuid.
    start_date = Date. "AAAA-MM-DD"
    end_date = Date. "AAAA-MM-DD"
    limit, page = int. Used for pagination
    user_account_id = uuid. account_id of the user that made the request. If user is tenant user its value is None
    """

    # Change value if active or inactive contract search
    if is_active == "enabled":
        is_active = "true"
    elif is_active == "disabled":
        is_active = "false"
    else:
        is_active = None

    # Query INNER JOIN to return account name
    sql = f"""SELECT c.contract_id, c.contract_name, c.account_id, c.start_date, 
        c.end_date, c.is_active , a.account_name 
        FROM contracts_master AS c INNER JOIN accounts_master AS a 
        ON c.account_id = a.account_id 
        WHERE c.tenant_id = '{tenant_id}' """

    sql_count = f"SELECT COUNT(*) FROM contracts_master AS c WHERE c.tenant_id = '{tenant_id}' "
    # Filter by values
    if contract_id is not None:
        sql += f"AND c.contract_id = '{contract_id}'"
        sql_count += f"AND c.contract_id = '{contract_id}'"
    elif contract_name is not None:
        sql += f"AND UNACCENT(c.contract_name) ILIKE UNACCENT('%{contract_name}%') "
        sql_count += f"AND UNACCENT(c.contract_name) ILIKE UNACCENT('%{contract_name}%') "

    # If the user that made the request is an account user, change the account_id to its account
    if user_account_id is not None:
        account_id = user_account_id

    if account_id is not None:
        sql += f" AND c.account_id = '{account_id}' "
        sql_count += f" AND c.account_id = '{account_id}' "

    if start_date is not None:
        sql += f" AND c.start_date >= '{start_date}' "
        sql_count += f" AND c.start_date >= '{start_date}' "
    if end_date is not None:
        sql += f" AND c.end_date <= '{end_date}' "
        sql_count += f" AND c.end_date <= '{end_date}' "

    if is_active is not None:
        sql += f" AND c.is_active = {is_active} "
        sql_count += f" AND c.is_active = {is_active} "

    # Pagination
    if limit is not None:
        limit = int(limit)
        offset = str(limit * (int(page) - 1))

        # ADD LIMIT AND OFFSET
        sql += f" LIMIT {limit} OFFSET {offset} "

    rds_response = rds_execute_statement(sql)
    contracts_data = deserialize_rds_response(rds_response)

    count = rds_execute_statement(sql_count)["records"][0][0]["longValue"]

    return (
        200,
        {
            "correlation_id": UUID,
            "count": count,
            "contracts": contracts_data,
            "result": "Get contracts success",
        },
    )


def get_data(data, query_parameters, tenant_id, user_account_id):
    # Return Contracts
    if data == "contracts":
        return get_contracts(
            **query_parameters, tenant_id=tenant_id, user_account_id=user_account_id
        )
    else:
        return (
            400,
            {"correlation_id": UUID, "result": "No action matches the specified data parameter"},
        )


def lambda_handler(event, context):
    try:
        if throttling_check():
            raise Exception("Throttling threshold exceeded")
        # Invoke previous version if requested
        LAMBDA_ARN = context.invoked_function_arn
        if "version" in event["headers"]:
            response = invoke_requested_version(LAMBDA_ARN, event)
            return response
        initialize_functions()
        send_to_coralogix(
            CORALOGIX_KEY,
            {"correlation_id": UUID, "Event Received": event},
            SERVICE_NAME,
            RESOURCE_METHOD,
            3,
        )
        # Use this for GET methods...
        query_parameters = event["queryStringParameters"]
        user_id, tenant_id = check_api_keys(event)
        account_id = get_account_id(user_id)
        # Check tenant level permissions (Adjunts the arguments of the function for your specific case)
        if check_tenant_level_permissions(tenant_id, "account", "contract", "general"):
            # Check user level permissions (Adjunts the arguments of the function for your specific case)
            if check_user_level_permissions(
                tenant_id, user_id, "account", "contract", "general", "can_read"
            ):
                # Create a function to perform the required action and returns a tuple with the status code in the first item and the json object in the second one
                response = get_data(
                    query_parameters.pop("data", None),
                    query_parameters,
                    tenant_id=tenant_id,
                    user_account_id=account_id,
                )
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
