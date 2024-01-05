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


def validate_data(account_id=None, products=[]):
    if account_id is None:
        return False, "PriceOverride.AccountIdRequired"

    if len(products) == 0:
        return False, "PriceOverride.ProductsRequired"

    has_product_id = all(["product_id" in product for product in products])

    if not has_product_id:
        return False, "PriceOverride.ProductIdRequired"

    return True, ""


def override_price(account_id=None, tenant_id=None, products=[], user_id=None):
    """
    Function to override the prices of products for an account
    """

    valid, code = validate_data(account_id, products)

    if not valid:
        return (400, {"correlation_id": UUID, "response": code})

    # Get currency from account
    sql = f"""SELECT currency_id FROM accounts_master 
    WHERE account_id = '{account_id}' """

    rds_response = rds_execute_statement(sql)
    currency_id = deserialize_rds_response(rds_response)[0]["currency_id"]

    account_product_price_overrides_ids = []

    # Iterate through products
    for product in products:
        product_id = product["product_id"]
        sql_get_currency_produts_id = f"""SELECT currency_products_id FROM currency_products 
            WHERE product_id = '{product_id}' AND currency_id = '{currency_id}' """

        rds_response = deserialize_rds_response(rds_execute_statement(sql_get_currency_produts_id))
        if len(rds_response):
            currency_products_id = rds_response[0]["currency_products_id"]
        else:
            sql_create_product_currency = f"""INSERT INTO currency_products (currency_id, product_id,
                 cost, created_by, updated_by) VALUES ('{currency_id}','{product_id}',0, '{user_id}', '{user_id}') RETURNING currency_products_id;"""
            currency_products_id = rds_execute_statement(sql_create_product_currency)["records"][0][
                0
            ]["stringValue"]

        # Check default price for currency

        sql = f"""
                SELECT cost FROM currency_products WHERE product_id = '{product["product_id"]}' AND currency_id = '{currency_id}'
            """
        rds_response = rds_execute_statement(sql)
        default_price = deserialize_rds_response(rds_response)[0]["cost"]

        # Check if price is already overridden
        sql = f"""SELECT COUNT(*) FROM account_product_price_overrides 
            WHERE account_id = '{account_id}' and product_id = '{product["product_id"]}'
            """
        count = rds_execute_statement(sql)["records"][0][0]["longValue"]

        if count:
            # If price is overridden and the new price is the same as the default currency price
            # Delete the override
            if default_price == product["price"] or product["price"] is None:
                sql == f"""DELETE FROM account_product_price_overrides 
                WHERE account_id = '{account_id}' and product_id = '{product["product_id"]}' 
                RETURNING account_product_price_overrides_id
                """
                account_product_price_overrides_ids.extend(
                    deserialize_rds_response(rds_execute_statement(sql))
                )

            else:
                # If default price for currency is different than the new value, update the override
                sql = f"""UPDATE account_product_price_overrides 
                SET product_price_override = {float(product["price"])} 
                WHERE account_id = '{account_id}' and product_id = '{product["product_id"]}' 
                RETURNING account_product_price_overrides_id
                """
                account_product_price_overrides_ids.extend(
                    deserialize_rds_response(rds_execute_statement(sql))
                )

        else:
            # If price is not overridden and the new value is different than the default price, Create the override
            if default_price != product["price"]:
                sql = f"""INSERT INTO account_product_price_overrides 
                (account_id, product_id, tenant_id, product_price_override, currency_id, currency_products_id ) 
                VALUES ('{account_id}', '{product["product_id"]}', '{tenant_id}', {product["price"]}, '{currency_id}', '{currency_products_id}' ) 
                RETURNING account_product_price_overrides_id 
                """
                account_product_price_overrides_ids.extend(
                    deserialize_rds_response(rds_execute_statement(sql))
                )

    return (
        200,
        {
            "correlation_id": UUID,
            "response": "Products Prices Overrode",
            "account_product_price_overrides_id": account_product_price_overrides_ids,
        },
    )


@webhook_dispatch(object_name="account_product_price_overrides", action="create")
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
        # Check tenant level permissions (Adjunts the arguments of the function for your specific case)
        if check_tenant_level_permissions(tenant_id, "admin", "users", "general"):
            # Check user level permissions (Adjunts the arguments of the function for your specific case)
            if check_user_level_permissions(
                tenant_id, user_id, "admin", "users", "general", "can_create"
            ):
                # Create a function to perform the required action and returns a tuple with the status code in the first item and the json object in the second one
                response = override_price(**event_body, user_id=user_id, tenant_id=tenant_id)
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
