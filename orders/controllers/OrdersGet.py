import base64
import json
import os
import sys
import uuid
from datetime import datetime, timedelta

import boto3

import core_utils.functions as functions

# import Values.
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


def get_products(order_id):
    sql_get_products = f"SELECT order_products_id, product_id, quantity, price, subtotal FROM order_products WHERE order_id = '{order_id}'"
    sql_get_products_count = f"SELECT COUNT(*) FROM order_products WHERE order_id = '{order_id}'"
    # gets a list of  products
    records = deserialize_rds_response(rds_execute_statement(sql_get_products))
    count = rds_execute_statement(sql_get_products_count)["records"][0][0]["longValue"]
    for product in records:
        product_id = product["product_id"]
        product_name, sku = get_product_name(product_id)
        product["product_name"] = product_name
        product["sku"] = sku
    return (count, records)


def get_product_name(product_id):
    sql_product_name = (
        f"SELECT product_name, sku FROM products_master WHERE product_id = '{product_id}'"
    )
    response = deserialize_rds_response(rds_execute_statement(sql_product_name))[0]
    product_name = response["product_name"]
    sku = response["sku"]
    return (product_name, sku)


def get_user_name(user_id):
    sql_get_user_name = f"SELECT full_name from users_master WHERE cognito_user_id = '{user_id}'"
    response = deserialize_rds_response(rds_execute_statement(sql_get_user_name))
    if len(response):
        return response[0]["full_name"]
    else:
        return "Tenant Key"


def get_account_currency(account_id):
    sql_get_account_currency = (
        f"SELECT currency_id FROM accounts_master WHERE account_id = '{account_id}'"
    )
    response = deserialize_rds_response(rds_execute_statement(sql_get_account_currency))
    print(response)
    if len(response):
        currency_id = response[0]["currency_id"]
        sql_get_currency = (
            f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
        )
        return deserialize_rds_response(rds_execute_statement(sql_get_currency))[0]["currency_name"]


def get_order_product_currency(order_products_id):
    sql_get_order_product_currency = (
        f"SELECT currency_id FROM order_products WHERE order_products_id = '{order_products_id}'"
    )
    response = deserialize_rds_response(rds_execute_statement(sql_get_order_product_currency))
    print(response)
    if len(response):
        currency_id = response[0]["currency_id"]
        sql_get_currency = (
            f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
        )
        return deserialize_rds_response(rds_execute_statement(sql_get_currency))[0]["currency_name"]


def get_order_product_currency(order_products_id):
    sql_get_order_product_currency = (
        f"SELECT currency_id FROM order_products WHERE order_products_id = '{order_products_id}'"
    )
    response = deserialize_rds_response(rds_execute_statement(sql_get_order_product_currency))
    print(response)
    if len(response):
        currency_id = response[0]["currency_id"]
        sql_get_currency = (
            f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
        )
        return deserialize_rds_response(rds_execute_statement(sql_get_currency))[0]["currency_name"]


def get_orders(
    user_id=None,
    order_number=None,
    order_name=None,
    order_id=None,
    start_date=None,
    end_date=None,
    account_name=None,
    account_number=None,
    status=None,
    created_by=None,
    product_id=None,
    created_to=None,
    created_from=None,
    limit=None,
    page=None,
    user_account_id=None,
    account_id=None,
    is_tenant_user=False,
):
    need_products = False
    print(account_name)
    print(is_tenant_user)

    # GET TIME ZONE FROM USER
    if user_id is not None:
        sql = f"""SELECT time_zone FROM users_master WHERE cognito_user_id = '{user_id}'"""
        rds_response = rds_execute_statement(sql)
        time_zone_list = deserialize_rds_response(rds_response)
        if len(time_zone_list):
            time_zone = time_zone_list[0]["time_zone"]
        else:
            time_zone = "UTC"
    else:
        time_zone = "UTC"

    # GET DATA FROM ORDER USING THE TIMEZONE OF THE USER
    sql_get_orders = f"""SELECT o.account_id, a.account_name, a.account_number, 
        o.order_id, o.order_number, o.order_name, o.status, 
        o.contract_id, o.start_date, o.end_date, o.total, o.created_by ,
        o.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}' AS created_at, 
        o.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}' AS updated_at
        FROM orders_master AS o INNER JOIN accounts_master AS a ON o.account_id = a.account_id """
    sql_get_orders_count = f"SELECT COUNT(*) FROM  orders_master AS o  "
    sql_additions = []

    if (
        order_id is not None
        or order_number is not None
        or order_name is not None
        or start_date is not None
        or end_date is not None
        or user_account_id is not None
        or account_name is not None
        or account_number is not None
        or status is not None
        or created_by is not None
        or product_id is not None
        or created_to is not None
        or created_from is not None
    ):
        sql_get_orders += " WHERE "
        sql_get_orders_count += " WHERE "

    if (
        order_id is not None
        and order_number is None
        and order_name is None
        and start_date is None
        and end_date is None
        and limit is None
        and page is None
    ):
        sql_order_id = f" o.order_id = '{order_id}'"
        sql_additions.append(sql_order_id)
        products_count, products_records = get_products(order_id)
        need_products = True

    else:
        if order_id is not None:
            sql_order_id = f" o.order_id::varchar LIKE '%{order_id}%'"
            sql_additions.append(sql_order_id)
        if order_number is not None:
            sql_order_number = f" o.order_number ILIKE '%{order_number}%'"
            sql_additions.append(sql_order_number)
        if order_name is not None:
            sql_order_name = f" UNACCENT(o.order_name) ILIKE UNACCENT('%{order_name}%')"
            sql_additions.append(sql_order_name)
        if start_date is not None and end_date is None:
            sql_start_date = f" o.created_at >= '{start_date}' "
            sql_additions.append(sql_start_date)
        if end_date is not None and start_date is None:
            sql_end_date = f" o.created_at <= '{end_date}' "
            sql_additions.append(sql_end_date)
        if account_name is not None and is_tenant_user == True:
            sql_account_name = f" o.account_id IN (SELECT account_id FROM accounts_master WHERE UNACCENT(account_name) ILIKE UNACCENT('%{account_name}%'))"
            sql_additions.append(sql_account_name)
            print("entra")
        if account_number is not None and is_tenant_user == True:
            sql_account_number = f" o.account_id IN (SELECT account_id FROM accounts_master WHERE account_number::varchar LIKE '%{int(account_number)}%')"
            sql_additions.append(sql_account_number)
        if status is not None and is_tenant_user == True:
            sql_status = f" o.status = '{status}'"
            sql_additions.append(sql_status)
        if product_id is not None and is_tenant_user == True:
            sql_product_id = f" o.order_id IN (SELECT order_id FROM order_products WHERE product_id::varchar LIKE '%{product_id}%')"
            sql_additions.append(sql_product_id)
        if created_by is not None and is_tenant_user == True:
            sql_created_by = f" o.created_by IN (SELECT cognito_user_id FROM users_master INNER JOIN orders_master ON users_master.cognito_user_id = orders_master.created_by WHERE UNACCENT(users_master.full_name) ILIKE UNACCENT('%{created_by}%'))"
            sql_additions.append(sql_created_by)
        if start_date is not None and end_date is not None and is_tenant_user == True:
            sql_created_from_to = f" o.created_at BETWEEN '{start_date}' AND '{end_date}'"
            sql_additions.append(sql_created_from_to)

    if user_account_id is not None:
        sql_account_id = f" o.account_id = '{user_account_id}'"
        sql_additions.append(sql_account_id)

    print(sql_additions)
    number_of_additions = len(sql_additions)
    addition_number = 1
    for addition in sql_additions:
        sql_get_orders += addition
        sql_get_orders_count += addition
        if number_of_additions > addition_number:
            sql_get_orders += " AND "
            sql_get_orders_count += " AND "
        addition_number += 1

    if limit is not None:
        limit = int(limit)
        offset = str(limit * (int(page) - 1))

        # ADD LIMIT AND OFFSET
        sql_get_orders += f" LIMIT {limit} OFFSET {offset} "

    count = rds_execute_statement(sql_get_orders_count)["records"][0][0]["longValue"]
    records = rds_execute_statement(sql_get_orders)
    print(sql_get_orders)
    records = list(deserialize_rds_response(records))

    for order in records:
        currency = get_account_currency(order["account_id"])
        user_id = order["created_by"]
        created_by = get_user_name(user_id)
        print(created_by)
        order["created_by"] = created_by
        order["currency"] = currency

    if need_products == True:
        print(products_records)
        for product in products_records:
            order_products_id = product["order_products_id"]
            currency = get_order_product_currency(order_products_id)
            product["currency"] = currency
        records = records[0]
        records["products"] = {"count": products_count, "records": products_records}

    return (count, records, time_zone)


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
        # user_id = "bda8c7ff-3d8a-4166-89d6-60c33a4c199a"
        # tenant_id = "4495ef98-176f-478c-900f-b9c67ddf206f"
        query_parameters = event["queryStringParameters"]
        account_id = get_account_id(user_id)
        if account_id is None and "account_id" in query_parameters:
            user_account_id = query_parameters["account_id"]
            query_parameters.pop("account_id")
            is_tenant_user = True
        elif account_id is None and "account_id" not in query_parameters:
            is_tenant_user = True
            user_account_id = None
        else:
            user_account_id = account_id
            is_tenant_user = False
        # GET BODY

        print(query_parameters)
        if check_tenant_level_permissions(tenant_id, "account", "order", "order_list"):
            # Check user level permissions
            if check_user_level_permissions(
                tenant_id, user_id, "account", "order", "order_list", "can_read"
            ):
                # here is the code to get the orders of an user
                if query_parameters is not None:
                    print(query_parameters)
                    count, records, time_zone = get_orders(
                        **query_parameters,
                        user_id=user_id,
                        user_account_id=user_account_id,
                        is_tenant_user=is_tenant_user,
                    )
                else:
                    count, records, time_zone = get_orders(
                        user_id=user_id,
                        user_account_id=user_account_id,
                        is_tenant_user=is_tenant_user,
                    )
                response = (
                    200,
                    {
                        "correlation_id": UUID,
                        "count": count,
                        "records": records,
                        "time_zone": time_zone,
                    },
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
