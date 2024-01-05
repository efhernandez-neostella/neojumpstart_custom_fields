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
    webhook_dispatch,
)


def initialize_functions():
    global UUID, CURRENT_DATETIME
    initialize()
    UUID = functions.UUID
    CURRENT_DATETIME = functions.CURRENT_DATETIME


def is_valid_contract_data(
    contract_id=None, contract_name=None, account_id=None, start_date=None, end_date=None
):
    """
    Validate Values for Contracts
    """
    # TODO: Add more validations?
    if contract_id is None:
        return False, "contract_id.ContractIdIsRequired"

    if contract_name is None:
        return False, "contract_name.ContractNameIsRequired"

    if account_id is None:
        return False, "account_id.AccountIdIsRequired"

    if start_date is None:
        return False, "start_date.StartDateIsRequired"

    if end_date is None:
        return False, "end_date.EndDateIsRequired"

    return True, "Success"


def update_contract(
    user_id=None,
    tenant_id=None,
    contract_id=None,
    contract_name=None,
    account_id=None,
    start_date=None,
    end_date=None,
    is_active=None,
):
    valid, code = is_valid_contract_data(
        contract_id, contract_name, account_id, start_date, end_date
    )
    """
    contract_id = uuid. Required. Id of the contract to be updated
    tenant_id = uuid. Required
    contract_name = str. 
    account_id = uuid. 
    start_date = Date. "AAAA-MM-DD". 
    end_date = Date. "AAAA-MM-DD". 
    is_active = Bool
    user_id = uuid. Required, user that updates the record
    """
    if not valid:
        return (403, {"correlation_id": UUID, "code": code})

    fields_data = []

    # Add field changes based on params values
    if contract_name != "" and contract_name is not None:
        fields_data.append(f" contract_name = '{contract_name}' ")

    if start_date != "" and start_date is not None:
        fields_data.append(f" start_date = '{start_date}' ")

    if end_date != "" and end_date is not None:
        fields_data.append(f" end_date = '{end_date}' ")

    if is_active != "" and is_active is not None:
        fields_data.append(f" is_active = '{is_active}' ")

    if len(fields_data):
        sql = f""" UPDATE contracts_master SET {",".join(fields_data)}, updated_by = '{user_id}', updated_at = NOW() 
            WHERE tenant_id = '{tenant_id}' AND account_id = '{account_id}' AND contract_id = '{contract_id}'
            """

        rds_execute_statement(sql)

    return (
        200,
        {
            "correlation_ids": UUID,
            "contract_id": contract_id,
            "response": "Contract Updated Successfully",
        },
    )


@webhook_dispatch(object_name="contracts_master", action="update")
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
        # Or use this for methods that include a body
        event_body = json.loads(event["body"])
        user_id, tenant_id = check_api_keys(event)
        account_id = get_account_id(user_id)
        # Check tenant level permissions (Adjunts the arguments of the function for your specific case)
        if check_tenant_level_permissions(tenant_id, "admin", "products", "general"):
            # Check user level permissions (Adjunts the arguments of the function for your specific case)
            if check_user_level_permissions(
                tenant_id, user_id, "admin", "products", "general", "can_update"
            ):
                # Create a function to perform the required action and returns a tuple with the status code in the first item and the json object in the second one
                if account_id is not None:
                    response = (
                        403,
                        {
                            "correlation_id": UUID,
                            "code": "user_permissions.UserDoesNotHaveAccessToThisFeature",
                        },
                    )
                else:
                    create_transaction()
                    response = update_contract(**event_body, user_id=user_id, tenant_id=tenant_id)
                    confirm_transaction()
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
