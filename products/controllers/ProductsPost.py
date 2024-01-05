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


def is_valid_product_data(product_name=None):
    if product_name is None:
        return False, "product_name.ProductNameIsRequired"

    return True, "Success"


# currency dict to translate currency to column name
currency_dict = {
    "USD": "product_cost_usd",
    "GBP": "product_cost_gbp",
    "COP": "product_cost_cop",
    "MXN": "product_cost_mxn",
    "BRL": "product_cost_brl",
}


def create_product(
    tenant_id=None,
    user_id=None,
    product_name=None,
    dinvy_id="",
    sku="",
    product_type="workato_recipe",
    description="",
    currencies=[],
    is_active=True,
    prorated=False,
    products=[],
    account_id=None,
):
    """
    Get all contracts filtered by values
    tenant_id = uuid. Required
    is_active = bool
    dinvy_id = str
    sku = str
    description = str
    currencies = array with json files like the following example
        {currency_id = uuid of the currency,
        cost= cost of the product to that currency}
    prorated = boola
    product_name = str.
    products = array with products_id and prices. Sent to override values
    account_id = uuid. Needed if you want to override the price of products for that account
    product_type = str. neostella_service, workato_recipe
    """

    # TODO: validation
    # valid, code = is_valid_product_data(product_name )

    # if not valid:
    #    return (403, {'correlation_id': UUID, 'code': code})

    sql = f""" INSERT INTO products_master(tenant_id, product_name, dinvy_id, product_type, 
            sku, description, prorated, is_active , created_by, updated_by) 
            VALUES('{tenant_id}', '{product_name}','{dinvy_id}', '{product_type}', 
            '{sku}', '{description}', {prorated}, {is_active}, '{user_id}', '{user_id}' ) 
            RETURNING product_id;
            """
    product_id = rds_execute_statement(sql)["records"][0][0]["stringValue"]
    for currency in currencies:
        currency_id = currency["currency_id"]
        currency_cost = currency["cost"]

        sql_create_product_currency = f"""INSERT INTO currency_products (currency_id, product_id,
                cost, created_by, updated_by) VALUES ('{currency_id}','{product_id}',{currency_cost}, '{user_id}', '{user_id}');"""
        rds_execute_statement(sql_create_product_currency)

    return (
        200,
        {
            "correlation_id": UUID,
            "product_id": product_id,
            "response": "Product Created Successfully",
        },
    )


@webhook_dispatch(object_name="products_master", action="create")
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
                tenant_id, user_id, "admin", "products", "general", "can_create"
            ):
                # Account Users can't create products
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
                    response = create_product(**event_body, user_id=user_id, tenant_id=tenant_id)
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
