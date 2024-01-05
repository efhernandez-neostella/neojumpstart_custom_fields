import base64
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import boto3

import core_utils.functions as functions

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

# import Values


def initialize_functions():
    global UUID, CURRENT_DATETIME
    initialize()
    UUID = functions.UUID
    CURRENT_DATETIME = functions.CURRENT_DATETIME


def is_contract_available(contract_id, start_date, end_date, order_id, time_zone):
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
    sql_start_date_order = f"SELECT start_date FROM orders_master where order_id = '{order_id}'"
    start_date_order_result = rds_execute_statement(sql_start_date_order)["records"][0][0][
        "stringValue"
    ].split(" ")[0]

    start_date_result = datetime.strptime(start_date_result, "%Y-%m-%d").date()
    end_date_result = datetime.strptime(end_date_result, "%Y-%m-%d").date()

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    start_date_order_result = datetime.strptime(start_date_order_result, "%Y-%m-%d").date()

    if start_date == start_date_order_result:
        today = start_date
    else:
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

    # print(today)
    if not is_active_result:
        return False, "ordersEdition.ContractIsNotActive"
    if start_date_result > start_date:
        return False, "ordersEdition.ContractStartDateIsGreaterThanOrderStartDate"
    if end_date_result < end_date:
        return False, "ordersEdition.ContractEndDateIsLessThanOrderEndDate"
    # TODO: Check this validation
    # if today > start_date:
    #    return False, "ordersEdition.OrderStartDateIsLessThanOriginalStartDate"

    return True, "Success"


def get_product_prorated(order_id, account_id, products=[]):
    sql = f"""SELECT start_date, end_date, contract_id FROM orders_master 
    WHERE order_id = '{order_id}' AND account_id = '{account_id}'"""
    rds_response = rds_execute_statement(sql)
    order_dates = deserialize_rds_response(rds_response)[0]
    order_start_date = datetime.strptime(order_dates["start_date"], "%Y-%m-%d")
    order_end_date = datetime.strptime(order_dates["end_date"], "%Y-%m-%d")
    order_number_days = order_end_date - order_start_date
    contract_id = order_dates["contract_id"]
    """
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
    for product in products:
        if product["prorated"]:
            prorated_value = (product["price"] / 365) * (int(order_number_days.days) + 1)
            product["price"] = round(prorated_value, 2)
    return products


def get_pricing_sheets(account_id, tenant_id, is_account_user=True, products=[]):
    pricing_sheet = []
    for product in products:
        product_data = {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "prorated": product["prorated"],
            "is_active": product["is_active"],
            "sku": product["sku"],
        }
        sql = f"""SELECT product_price_override FROM account_product_price_overrides 
        WHERE account_id = '{account_id}' and product_id = '{product["product_id"]}'
        """
        rds_response = rds_execute_statement(sql)
        price_override_data = deserialize_rds_response(rds_response)
        if len(price_override_data):
            product_data["price"] = price_override_data[0]["product_price_override"]
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
            currency = deserialize_rds_response(rds_execute_statement(sql_get_currency))[0][
                "currency_name"
            ]
            product_id = product_data["product_id"]
            sql_get_price = f"SELECT cost FROM currency_products WHERE product_id = '{product_id}' AND currency_id = '{currency_id}'"
            product_cost = deserialize_rds_response(rds_execute_statement(sql_get_price))[0]["cost"]
            # set the price based on the currency of the account
            product_data["price"] = product_cost

        pricing_sheet.append(product_data)
    return pricing_sheet


def get_order_products(order_id):
    sql_get_order_products = f"SELECT product_id FROM order_products WHERE order_id = '{order_id}'"
    response = deserialize_rds_response(rds_execute_statement(sql_get_order_products))
    return response


"""
def order_has_products(order_id):
    sql_get_count_products = f"SELECT COUNT(*) FROM order_products WHERE order_id = '{order_id}'"
    result = rds_execute_statement(sql_get_count_products)["records"][0][0]["longValue"]
    if result == 0:
        return False
    else: 
        return True
"""


def get_products(products_id):
    print("esta entrando aqui2")
    sql_get_products = f"SELECT * FROM products_master WHERE"
    sql_additions = []

    for product_id in products_id:
        product_id = product_id["product_id"]
        sql_product_id = f" product_id = '{product_id}'"
        sql_additions.append(sql_product_id)

    number_of_additions = len(sql_additions)
    addition_number = 1
    for addition in sql_additions:
        sql_get_products += addition
        if number_of_additions > addition_number:
            sql_get_products += " OR "
        addition_number += 1
    response = deserialize_rds_response(rds_execute_statement(sql_get_products))
    return response


def update_products_price(order_id, user_account_id, tenant_id, products_id=[]):
    products_list = get_products(products_id)
    products_list = get_pricing_sheets(
        user_account_id, tenant_id, is_account_user=True, products=products_list
    )
    products_list = get_product_prorated(order_id, user_account_id, products=products_list)
    for product in products_list:
        product_id = product["product_id"]
        price = product["price"]
        if price == "0.0":
            price = 0
        sql_update_price = f"UPDATE order_products SET price = {price} WHERE order_id = '{order_id}' AND product_id = '{product_id}'"
        rds_execute_statement(sql_update_price)
    return "products updated"


def get_contract_id(order_id):
    sql_get_contract_id = f"SELECT contract_id FROM orders_master WHERE order_id = '{order_id}'"
    response = deserialize_rds_response(rds_execute_statement(sql_get_contract_id))[0][
        "contract_id"
    ]
    return response


def is_a_draft_order(order_id):
    sql_is_a_draft_order = (
        f"SELECT COUNT(*) FROM orders_master WHERE order_id = '{order_id}' AND status = 'draft'"
    )
    response = rds_execute_statement(sql_is_a_draft_order)["records"][0][0]["longValue"]
    if response == 0:
        return False
    else:
        return True


def get_dates(order_id, date_type):
    sql_get_contract_id = f"SELECT {date_type} FROM orders_master WHERE order_id = '{order_id}'"
    response = deserialize_rds_response(rds_execute_statement(sql_get_contract_id))[0][date_type]
    return response


def validate_products(order_id, products_id=[]):
    # Check if there are no active products associated to the order
    sql_check_active_products = f"""SELECT COUNT(*) FROM products_master 
    WHERE product_id IN ('{"','".join(products_id)}') AND is_active = false"""
    count = rds_execute_statement(sql_check_active_products)["records"][0][0]["longValue"]

    if count:
        return False, "ordersEdition.ProductNotAvailable"

    # Check if there are products with negative or 0 quantity associated to the order
    sql_check_active_products = f"""SELECT COUNT(*) FROM order_products WHERE order_id = 
    '{order_id}' AND quantity < 1 """
    count = rds_execute_statement(sql_check_active_products)["records"][0][0]["longValue"]

    if count:
        return False, "ordersEdition.ProductQuantityMustBeAtLeast1"

    sql_get_account = f"""SELECT account_id FROM orders_master WHERE order_id = '{order_id}'"""
    account_id = deserialize_rds_response(rds_execute_statement(sql_get_account))[0]["account_id"]
    sql_get_account = (
        f"""SELECT currency_id FROM accounts_master WHERE account_id = '{account_id}'"""
    )
    account_currency_id = deserialize_rds_response(rds_execute_statement(sql_get_account))[0][
        "currency_id"
    ]
    sql_get_order_products = f"SELECT * FROM order_products WHERE order_id = '{order_id}'"
    products = deserialize_rds_response(rds_execute_statement(sql_get_order_products))
    for product in products:
        if product["currency_id"] != account_currency_id:
            return False, "ordersEdition.ProductCurrencyDoesNotMatch"

    return True, "Success"


def update_order(
    action="edit",
    contract_id=None,
    order_id=None,
    order_name=None,
    start_date=None,
    end_date=None,
    account_id=None,
    user_id=None,
    user_account_id=None,
    tenant_id=None,
):
    # replace ' with '' to not have error in the query
    if order_name is not None:
        order_name = str(order_name).replace(r"'", r"''")

    sql_update_order = "UPDATE orders_master SET "
    sql_additions = []
    date_change = False

    sql = f"SELECT time_zone FROM users_master WHERE cognito_user_id = '{user_id}'"
    response = rds_execute_statement(sql)["records"]
    if len(response):
        time_zone = response[0][0]["stringValue"]
    else:
        time_zone = "UTC"

    if is_a_draft_order(order_id):
        if order_name is not None:
            sql_update_order_name = f"order_name = '{order_name}'"
            sql_additions.append(sql_update_order_name)
        if start_date is not None or end_date is not None:
            date_change = True
            if contract_id is not None:
                sql_update_order_contract_id = f"contract_id = '{contract_id}'"
                sql_additions.append(sql_update_order_contract_id)
            else:
                contract_id = get_contract_id(order_id)
            if start_date is None:
                start_date = get_dates(order_id, "start_date")
            if end_date is None:
                end_date = get_dates(order_id, "end_date")
            valid_contract, code = is_contract_available(
                contract_id, start_date, end_date, order_id, time_zone
            )
            if valid_contract:
                sql_update_order_dates = f"start_date = '{start_date}', end_date = '{end_date}'"
                sql_additions.append(sql_update_order_dates)
            else:
                return (400, {"correlation_id": UUID, "code": code})
    else:
        return (403, {"correlation_id": UUID, "code": "ordersEdition.OrderNotDraft"})

    products_id = get_order_products(order_id)

    if action == "submit":
        # check there are at least one product associated to the order
        if len(products_id) < 1:
            return (
                403,
                {"correlation_id": UUID, "code": "ordersEdition.OrderRequireProductsToBeSubmitted"},
            )

        valid, code = validate_products(
            order_id, [product["product_id"] for product in products_id]
        )

        if not valid:
            return (403, {"correlation_id": UUID, "code": code})

        # Validate dates of contract and order
        if start_date is None:
            start_date = get_dates(order_id, "start_date")
        if end_date is None:
            end_date = get_dates(order_id, "end_date")

        valid_contract, code = is_contract_available(
            contract_id, start_date, end_date, order_id, time_zone
        )
        if not valid_contract:
            return (400, {"correlation_id": UUID, "code": code})

        sql_update_order_status = f"status = 'submitted'"
        sql_additions.append(sql_update_order_status)

    if len(sql_additions):
        sql_update_updated_at = f"updated_at = NOW()"
        sql_additions.append(sql_update_updated_at)
        sql_update_updated_by = f"updated_by = '{user_id}'"
        sql_additions.append(sql_update_updated_by)

    number_of_additions = len(sql_additions)
    addition_number = 1
    for addition in sql_additions:
        sql_update_order += addition
        if number_of_additions > addition_number:
            sql_update_order += " , "
        addition_number += 1
    if date_change and len(products_id):
        products = update_products_price(
            order_id, user_account_id, tenant_id, products_id=products_id
        )
    sql_update_order += f" WHERE order_id = '{order_id}'"
    result = rds_execute_statement(sql_update_order)

    # SEND SUBMITTED NOTIFICATION TO WORKATO
    """
    if action == "submit":
        sql = f""SELECT account_name FROM accounts_master 
        WHERE account_id = '{user_account_id}' ""
        rds_response = rds_execute_statement(sql)
        account_name = deserialize_rds_response(rds_response)[0]["account_name"]
        params = {
            "UUID": UUID,
            "OrderID": order_id,
            "AccountName": account_name
        }
        execute_workato(params)
    """

    print("aqui entra el codigo")
    response = (200, {"correlation_id": UUID, "order_id": order_id, "code": "order updated"})
    return response


@webhook_dispatch(object_name="orders_master", action="update")
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
        print(event_body)
        print(account_id)
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
                # here is the code to get the orders of an user
                create_transaction()
                response = update_order(
                    **event_body,
                    user_account_id=user_account_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                )
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
