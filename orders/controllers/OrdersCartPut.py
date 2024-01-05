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


def update_total(order_id: str):
    # update total
    sql_calculate_total = f"SELECT COALESCE(SUM(subtotal), 0) AS total FROM order_products WHERE order_id = '{order_id}';"
    records = deserialize_rds_response(rds_execute_statement(sql_calculate_total))
    total = records[0]["total"]
    sql_update_total = f"UPDATE orders_master SET total = '{total}' WHERE order_id = '{order_id}'"
    rds_execute_statement(sql_update_total)


def initialize_functions():
    global UUID, CURRENT_DATETIME
    initialize()
    UUID = functions.UUID
    CURRENT_DATETIME = functions.CURRENT_DATETIME


def check_values(order_id=None, product_id=None, quantity=None, action=None):
    if order_id is None:
        return False, "ordersEdition.OrderIdNeeded"
    if product_id is None:
        return False, "ordersEdition.ProductIdNeeded"
    if quantity is None and action == "update":
        return False, "ordersEdition.ProductQuantityNeeded"

    return True, "Success"


def account_verification(order_id, user_account_id):
    sql_account_verification = f"SELECT COUNT(*) FROM orders_master WHERE order_id = '{order_id}' AND account_id = '{user_account_id}' "
    response = rds_execute_statement(sql_account_verification)["records"][0][0]["longValue"]
    print(response)
    if response == 0:
        return False
    else:
        return True


def edit_product(
    order_products_id=None,
    action=None,
    order_id=None,
    product_id=None,
    quantity=None,
    user_account_id=None,
    user_id=None,
):
    is_valid, code = check_values(
        order_id=order_id, product_id=product_id, quantity=quantity, action=action
    )

    if not is_valid:
        return (400, {"correlation_id": UUID, "code": code})

    if account_verification(order_id, user_account_id):
        if action == "update":
            # calculate the update subtotal
            sql_get_subtotal = (
                f"SELECT * from order_products where order_products_id = '{order_products_id}'"
            )
            records_subtotal = deserialize_rds_response(rds_execute_statement(sql_get_subtotal))
            subtotal = 0
            for price in records_subtotal:
                subtotal += price["price"]
            subtotal = subtotal * quantity
            sql_edit_product = f"UPDATE order_products SET quantity = {quantity},subtotal = {subtotal} ,updated_by = '{user_id}', updated_at = NOW() WHERE order_id = '{order_id}' AND product_id = '{product_id}' RETURNING order_products_id;"
            result = rds_execute_statement(sql_edit_product)
            order_products_id = deserialize_rds_response(result)[0]["order_products_id"]
            code = "Product update"

        elif action == "delete":
            sql_delete_products = f"DELETE FROM order_products WHERE order_id = '{order_id}' AND product_id = '{product_id}' RETURNING order_products_id;"
            order_products_id = deserialize_rds_response(rds_execute_statement(sql_delete_products))
            order_products_id = (
                order_products_id[0]["order_products_id"] if len(order_products_id) else None
            )
            code = "Product delete"

        update_total(order_id)
        response = (
            200,
            {
                "correlation_id": UUID,
                "order_products_id": order_products_id,
                "code": code,
            },
        )

    else:
        response = (
            403,
            {"correlation_id": UUID, "code": "shoppingCart.OrderDoesNotBelongsToTheAccount"},
        )
    return response


@webhook_dispatch(object_name="order_products", action="update")
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
        query_parameters = event["queryStringParameters"]
        event_body = json.loads(event["body"])

        user_id, tenant_id = check_api_keys(event)
        print(tenant_id)

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
                tenant_id, user_id, "account", "order", "general", "can_update"
            ):
                # here is the code to get the orders of an user

                create_transaction()
                result = edit_product(
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
        print(ERROR_MSG)
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
