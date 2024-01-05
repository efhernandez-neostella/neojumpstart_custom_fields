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


def is_valid_product_data(product_id=None):
    if product_id is None:
        return False, "product_name.ProductIdIsRequired"

    return True, "Success"


def update_product(
    tenant_id=None,
    user_id=None,
    product_id=None,
    product_name=None,
    product_type=None,
    dinvy_id=None,
    sku=None,
    description=None,
    product_cost={},
    prorated=None,
    is_active=True,
):
    """
    Get all contracts filtered by values
    product_id = uuid
    tenant_id = uuid. Required
    is_active = bool
    dinvy_id = str
    sku = str
    description = str
    product_cost_usd = float
    product_cost_gbp = float
    product_cost_cop = float
    product_cost_mxn = float
    product_cost_brl = float
    prorated = boola
    product_name = str.
    product_type = str. neostella_service, workato_recipe
    """

    valid, code = is_valid_product_data(product_id)
    if not valid:
        return (403, {"correlation_id": UUID, "code": code})

    # Add the changed to the array
    fields_data = []

    if product_name is not None:
        fields_data.append(f"product_name = '{product_name}' ")
    if dinvy_id is not None:
        fields_data.append(f"dinvy_id = '{dinvy_id}' ")
    if sku is not None:
        fields_data.append(f"sku = '{sku}' ")

    if description is not None:
        fields_data.append(f"description = '{description}' ")

    if product_type is not None:
        fields_data.append(f"product_type = '{product_type}' ")

    if product_cost != {}:
        #    fields_data.append(f"product_cost_usd = {float(product_cost_usd)} ")
        sql_get_current_currencies = (
            f"SELECT currency_id FROM currency_products WHERE product_id = '{product_id}'"
        )
        currencies_id_response = deserialize_rds_response(
            rds_execute_statement(sql_get_current_currencies)
        )
        currencies_id = []
        for currency in currencies_id_response:
            currencies_id.append(currency["currency_id"])
        product_cost_currency = product_cost.keys()
        new_currencies_id = []
        for cost_currency in product_cost_currency:
            currency = cost_currency.split("_")[2]
            sql_get_currency_id = f"SELECT currency_id FROM currencies_master WHERE currency_name = UPPER('{currency}')"
            currency_id = deserialize_rds_response(rds_execute_statement(sql_get_currency_id))[0][
                "currency_id"
            ]
            new_currencies_id.append(currency_id)
            cost = product_cost[cost_currency]
            if float(cost) < 0:
                return (400, {"correlation_id": UUID, "response": "No product field was changed"})
            else:
                if currency_id in currencies_id:
                    sql_update_cost = f"UPDATE currency_products SET cost = {cost}, updated_by = '{user_id}', updated_at=NOW() WHERE product_id = '{product_id}' AND currency_id = '{currency_id}'"
                    rds_execute_statement(sql_update_cost)
                elif cost != 0:
                    sql_create_cost = f"""INSERT INTO currency_products (currency_id, product_id,
                    cost, created_by, updated_by) VALUES ('{currency_id}','{product_id}',{cost}, '{user_id}', '{user_id}');"""
                    rds_execute_statement(sql_create_cost)
        for old_currency_id in currencies_id:
            if old_currency_id not in new_currencies_id:
                sql_delete_cost = f"""DELETE FROM currency_products WHERE product_id = '{product_id}' AND currency_id = '{old_currency_id}';"""
                rds_execute_statement(sql_delete_cost)

    if prorated is not None:
        fields_data.append(f"prorated = {prorated} ")
    if is_active is not None:
        fields_data.append(f"is_active = {is_active} ")

    # Update query if there was any change
    if fields_data:
        sql = f""" UPDATE products_master SET {" , ".join(fields_data)} , 
                updated_by = '{user_id}', updated_at=NOW() 
                WHERE product_id = '{product_id}' AND tenant_id = '{tenant_id  }'
            """
        rds_execute_statement(sql)

        return (
            200,
            {
                "correlation_id": UUID,
                "product_id": product_id,
                "response": "Product Updated Successfully",
            },
        )

    else:
        return (400, {"correlation_id": UUID, "response": "No product field was changed"})


@webhook_dispatch(object_name="products_master", action="update")
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
                # Account users can't update products
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
                    response = update_product(**event_body, user_id=user_id, tenant_id=tenant_id)
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
