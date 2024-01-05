import base64
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from multiprocessing import current_process

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


def get_product_prorated(order_id, account_id, products=[]):
    # get dates from the order
    sql = f"""SELECT start_date, end_date, contract_id FROM orders_master 
    WHERE order_id = '{order_id}' AND account_id = '{account_id}'"""

    rds_response = rds_execute_statement(sql)
    order_dates = deserialize_rds_response(rds_response)[0]

    # order_start_date = datetime.strptime(order_dates["start_date"], '%Y-%m-%d')
    order_start_date = datetime.strptime(order_dates["start_date"], "%Y-%m-%d")
    order_end_date = datetime.strptime(order_dates["end_date"], "%Y-%m-%d")
    order_number_days = order_end_date - order_start_date

    """
    contract_id = order_dates["contract_id"]

    sql = f"
        SELECT start_date, end_date FROM contracts_master 
        WHERE contract_id = '{contract_id}' AND account_id = '{account_id}'
    "

    rds_response = rds_execute_statement(sql)
    contract_dates = deserialize_rds_response(rds_response)[0]

    contract_start_date = datetime.strptime(contract_dates["start_date"],'%Y-%m-%d')
    contract_end_date = datetime.strptime(contract_dates["end_date"],  '%Y-%m-%d')
    contract_number_days = contract_end_date - contract_start_date
    """

    # Check for every product if it is prorated and calculate the price
    for product in products:
        if product["prorated"]:
            prorated_value = (product["price"] / 365) * (int(order_number_days.days) + 1)
            product["price"] = round(prorated_value, 2)

    return products


def get_pricing_sheets(account_id, tenant_id, is_account_user=True, products=[]):
    pricing_sheet = []
    for product in products:
        # dictionary for each product data
        product_data = {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_type": product["product_type"],
            "prorated": product["prorated"],
            "is_active": product["is_active"],
            "sku": product["sku"],
            "description": product["description"],
        }
        sql = f"""SELECT product_price_override FROM account_product_price_overrides 
        WHERE account_id = '{account_id}' and product_id = '{product["product_id"]}'
        """
        # check if product has the price overridden
        rds_response = rds_execute_statement(sql)
        price_override_data = deserialize_rds_response(rds_response)
        print(price_override_data)

        # If price is overridden, add it to the product data
        if len(price_override_data):
            product_data["price"] = price_override_data[0]["product_price_override"]
            # if the user that made the request is a tenant user, the product will say if it is overriden
            if not is_account_user:
                product_data["overridden"] = True
        else:
            if not is_account_user:
                product_data["overridden"] = False

            # Get currency of the account
            sql = f"""SELECT currency_id FROM accounts_master 
            WHERE account_id = '{account_id}' """
            rds_response = rds_execute_statement(sql)
            currency_id = deserialize_rds_response(rds_response)[0]["currency_id"]
            sql_get_currency = (
                f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
            )
            currency_name = deserialize_rds_response(rds_execute_statement(sql_get_currency))[0][
                "currency_name"
            ]
            product_id = product_data["product_id"]
            sql_get_price = f"SELECT cost FROM currency_products WHERE product_id = '{product_id}' AND currency_id = '{currency_id}'"
            product_cost = deserialize_rds_response(rds_execute_statement(sql_get_price))
            print(product_cost)
            if len(product_cost):
                # set the price based on the currency of the account
                product_data["price"] = product_cost[0]["cost"]
            else:
                product_data["price"] = 0
            product_data["currency"] = currency_name

        pricing_sheet.append(product_data)

    return pricing_sheet


def get_products(
    tenant_id=None,
    account_id=None,
    is_account_user=True,
    order_id=None,
    product_type=None,
    product_id=None,
    product_name=None,
    is_active=None,
    limit=None,
    page=None,
):
    """
    Get all contracts filtered by values
    tenant_id = uuid. Required
    is_active = enabled/disabled
    product_name = str.
    is_account_user: Bool. If true, when the price is overrode it won't return a key telling the product price is overridden
    account_id = uuid. Needed if you want to get the overrode price of the products for that account
    order_id = uuid. Needed if you want to get the prorated price of the products for that order
    product_type = str. neostella_service, workato_recipe
    limit, page = int. Used for pagination
    user_account_id = uuid. account_id of the user that made the request. If user is tenant user its value is None
    """

    if is_active == "enabled":
        is_active = "true"
    elif is_active == "disabled":
        is_active = "false"
    else:
        is_active = None

    sql = f"""SELECT product_id, product_name, 
        dinvy_id, sku, product_type,  description,
        prorated, is_active FROM products_master 
        WHERE tenant_id = '{tenant_id}' """
    sql_count = f"""SELECT COUNT(*) 
        FROM products_master WHERE tenant_id = '{tenant_id}' """

    # Filter by values
    if product_id is not None:
        # Leaving comment below so in the future if the product has more data,
        # the query can be changed to return all the info or only given data
        # query = query.replace("UUID,product_id,name", "*")
        sql += f"AND product_id = '{product_id}'"
        sql_count += f"AND product_id = '{product_id}'"
    elif product_name is not None:
        sql += f" AND UNACCENT(product_name) ILIKE UNACCENT('%{product_name}%') "
        sql_count += f" AND UNACCENT(product_name) ILIKE UNACCENT('%{product_name}%') "

    if product_type is not None:
        sql += f" AND UNACCENT(product_type) ILIKE UNACCENT('%{product_type}%') "
        sql_count += f" AND UNACCENT(product_type) ILIKE UNACCENT('%{product_type}%') "

    if is_active is not None:
        sql += f" AND is_active = {is_active}"
        sql_count += f" AND is_active = {is_active}"

    # Pagination
    if limit is not None:
        limit = int(limit)
        offset = str(limit * (int(page) - 1))

        # ADD LIMIT AND OFFSET
        sql += f" LIMIT {limit} OFFSET {offset} "

    rds_response = rds_execute_statement(sql)
    products_data = deserialize_rds_response(rds_response)

    for product in products_data:
        product_id = product["product_id"]
        sql_get_products = (
            f"SELECT currency_id, cost from currency_products where product_id = '{product_id}'"
        )

        product_costs = deserialize_rds_response(rds_execute_statement(sql_get_products))
        currencies_added = []
        for product_cost in product_costs:
            currency_id = product_cost["currency_id"]
            currencies_added.append(currency_id)
            sql_get_currency_name = (
                f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
            )
            currency_name = deserialize_rds_response(rds_execute_statement(sql_get_currency_name))[
                0
            ]["currency_name"]
            currency_product_name = "product_cost_" + currency_name.lower()
            # print(currency_product_name)
            product[currency_product_name] = product_cost["cost"]
            # product["currency"] = currency_name
        sql_get_currencies = f"SELECT currency_id, currency_name FROM currencies_master WHERE tenant_id = '{tenant_id}'"
        currencies_id = deserialize_rds_response(rds_execute_statement(sql_get_currencies))
        for currency in currencies_id:
            if currency["currency_id"] not in currencies_added:
                currency_product_name = "product_cost_" + currency["currency_name"].lower()
                product[currency_product_name] = 0

    currency = None

    # If account_id is sent, return the overridden price of the products
    if account_id is not None:
        products_data = get_pricing_sheets(account_id, tenant_id, is_account_user, products_data)

        sql = f"""SELECT currency_id FROM accounts_master WHERE account_id = '{account_id}'"""

        rds_response = rds_execute_statement(sql)
        currency_response = deserialize_rds_response(rds_response)

        # return the currency of the account
        if len(currency_response):
            currency_id = currency_response[0]["currency_id"]
            sql_get_currency_name = (
                f"SELECT currency_name FROM currencies_master WHERE currency_id = '{currency_id}'"
            )
            currency = deserialize_rds_response(rds_execute_statement(sql_get_currency_name))[0][
                "currency_name"
            ]

        # If the order_id is sent, return the prorated price of the products that can be prorated
        if order_id is not None:
            products_data = get_product_prorated(order_id, account_id, products_data)

    count = rds_execute_statement(sql_count)["records"][0][0]["longValue"]

    dict_response = {
        "correlation_id": UUID,
        "count": count,
        "products": products_data,
        "result": "Get products success",
    }

    if currency is not None:
        dict_response["currency"] = currency

    return (200, dict_response)


def get_data(data, query_parameters, tenant_id, account_id, is_account_user):
    # Return products
    if data == "products":
        return get_products(
            **query_parameters,
            tenant_id=tenant_id,
            account_id=account_id,
            is_account_user=is_account_user,
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
        is_account_user = True
        # Check tenant level permissions (Adjunts the arguments of the function for your specific case)
        if check_tenant_level_permissions(tenant_id, "admin", "products", "general"):
            # Check user level permissions (Adjunts the arguments of the function for your specific case)
            # If the user is an account user, the pricing_sheet permission must be checked
            pricing_sheet_access = False
            if account_id is not None:
                query_parameters.pop("account_id", None)
                pricing_sheet_access = check_user_level_permissions(
                    tenant_id, user_id, "account", "pricing_sheets", "general", "can_read"
                )
            # Check product permission
            if (
                check_user_level_permissions(
                    tenant_id, user_id, "admin", "products", "general", "can_read"
                )
                or pricing_sheet_access
            ):
                # Create a function to perform the required action and returns a tuple with the status code in the first item and the json object in the second one
                # check if it is an account_user
                if account_id is None:
                    is_account_user = False
                    account_id = query_parameters.pop("account_id", None)
                response = get_data(
                    query_parameters.pop("data", None),
                    query_parameters,
                    tenant_id=tenant_id,
                    account_id=account_id,
                    is_account_user=is_account_user,
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
