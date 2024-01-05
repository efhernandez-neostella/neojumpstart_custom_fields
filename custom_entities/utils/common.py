import json
import uuid
from decimal import Decimal
from typing import Dict, List

import boto3
from boto3.dynamodb.types import TypeDeserializer

from core_utils.functions import (
    CORALOGIX_KEY,
    REGION_NAME,
    RESOURCE_METHOD,
    SERVICE_NAME,
    UUID,
    deserialize_rds_response,
    rds_execute_statement,
    send_to_coralogix,
)
from sql_handler.sql_tables_handler import SQLTable
from utils.exceptions.controller_exceptions import ControllerException

DYNAMO_CLIENT = boto3.client("dynamodb", region_name=REGION_NAME)


def get_user_timezone(user_id: str) -> str:
    default_time_zone = "UTC"
    sql = SQLTable(f"SELECT time_zone FROM users_master WHERE cognito_user_id = '{user_id}'").query
    rds_response = rds_execute_statement(sql)
    time_zone_list = deserialize_rds_response(rds_response)

    if len(time_zone_list):
        return time_zone_list[0]["time_zone"]
    return default_time_zone


def format_audit(data: List[Dict]) -> List[Dict]:
    for row in data:
        row["audit_data"] = {
            "created": {
                "created_by_user_id": row.pop("created_by", None),
                "created_by_user_name": row.pop("created_by_user_name", None),
                "created_at": row.pop("created_at", None),
            },
            "updated": {
                "updated_by_user_id": row.pop("updated_by"),
                "updated_by_user_name": row.pop("updated_by_user_name", None),
                "updated_at": row.pop("updated_at", None),
            },
        }


def _check_str_characters(value: str) -> bool:
    if "'" in value:
        return True
    return False


def parse_str_characters(value: str) -> str:
    if _check_str_characters(value):
        return value.replace(r"'", r"''")
    return value


def format_for_rds(value) -> str:
    # if value is None or value == "": TODO: check if in empty Strings we want to replace to null
    if value is None:
        return "NULL"
    elif isinstance(value, str) and value != "NOW()" and value != "DEFAULT":
        parsed_value = parse_str_characters(value)
        return f"'{parsed_value}'"
    elif isinstance(value, dict) or isinstance(value, list):
        return f"'{json.dumps(value)}'"
    else:
        return f"{value}"


def parse_insert_data_rds(data: dict) -> dict:
    return {key: format_for_rds(value) for key, value in data.items()}


def parse_update_data_rds(data: dict) -> list:
    return [f"{key} = {format_for_rds(value)}" for key, value in data.items()]


def format_nullable_params(value) -> str:
    if value is None:
        return "NULL"
    return f"'{value}'"


def format_nulleable_values(value):
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        if "'" in value:
            value = rf"{value}".replace("'", r"\'")
            return f"e'{value}'"
        else:
            return f"'{value}'"
    elif isinstance(value, bool) or isinstance(value, int) or isinstance(value, float):
        return f"{value}"
    elif isinstance(value, dict):
        return f"'{json.dumps(value)}'"


def check_if_database_columns_exist():
    sql = """SELECT count(*) 
            FROM information_schema.columns 
            WHERE table_name='tenants_master' and column_name='database_name';"""
    count_result = deserialize_rds_response(rds_execute_statement(sql=sql))[0]["count"]
    return count_result > 0


def get_tenants_db_credentials(from_objects=False):
    default_credentials = {"tenant_id": None, "rds_params": {}, "transaction_params": {}}
    credentials = []
    database_columns_exist = check_if_database_columns_exist()
    if database_columns_exist:
        sql = """
            SELECT tenant_id, database_name, db_cluster_arn, db_credentials_secrets_store_arn 
            FROM tenants_master
        """
        data = deserialize_rds_response(rds_execute_statement(sql))
    elif not from_objects:
        data = []
    else:
        sql = """
            SELECT tenant_id, Null as database_name, Null as db_cluster_arn, 
            Null as db_credentials_secrets_store_arn FROM tenants_master
        """
        data = deserialize_rds_response(rds_execute_statement(sql))

    if len(data) == 0:
        # Use default credentials
        credentials.append(default_credentials)
        return credentials
    else:
        for credential in data:
            if credential["database_name"] is None:
                # Use default credentials for master tenant
                credentials.append(
                    {
                        "tenant_id": credential["tenant_id"],
                        "rds_params": {},
                        "transaction_params": {},
                    }
                )
            else:
                # Use default db credentials for new tenants
                credentials.append(
                    {
                        "tenant_id": credential["tenant_id"],
                        "rds_params": {
                            "database": credential["database_name"],
                            "resourceArn": credential["db_cluster_arn"],
                            "secretArn": credential["db_credentials_secrets_store_arn"],
                        },
                        "transaction_params": {
                            "resourceArn": credential["db_cluster_arn"],
                            "secretArn": credential["db_credentials_secrets_store_arn"],
                        },
                    }
                )

        return credentials


def handle_payload_error(valid: bool, code: str) -> None:
    if not valid:
        raise ControllerException(
            400,
            {
                "correlation_id": UUID,
                "result": "There is wrong parameters in the request body.",
                "code": code,
            },
        )


def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}


def dynamo_execute_statement(query):
    send_to_coralogix(
        CORALOGIX_KEY, {"UUID": UUID, "Query string": query}, SERVICE_NAME, RESOURCE_METHOD, 3
    )
    response = DYNAMO_CLIENT.execute_statement(Statement=query)
    data = response["Items"]

    while "NextToken" in response:
        response = DYNAMO_CLIENT.execute_statement(Statement=query, NextToken=response["NextToken"])
        data.extend(response["Items"])

    result = []
    for item in data:
        result.append(from_dynamodb_to_json(item))

    return result


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            n = float(o)
            if n.is_integer():
                return int(n)
            else:
                return n
        return super(DecimalEncoder, self).default(o)


def get_object_custom_fields(object_name: str):
    sql = SQLTable(
        f"""SELECT column_name FROM information_schema.columns where table_name = '{object_name}' 
        and column_name LIKE 'cf_%';"""
    ).query
    columns = deserialize_rds_response(rds_execute_statement(sql))
    return columns


def str_columns_parser(array, append=False):
    str_columns = ",".join(array)
    if append:
        if len(str_columns) > 0:
            str_columns = "," + str_columns
    return str_columns


def get_select_cf_columns(object_name: str, array: bool = False, append=False):
    columns = get_object_custom_fields(object_name=object_name)
    array_columns = [f"{object_name}.{column['column_name']}" for column in columns]
    if array:
        return array_columns
    str_columns = str_columns_parser(array_columns, append)
    return str_columns


def insert_cf_columns(object_name: str, payload: dict, array: bool, append=False):
    columns = get_object_custom_fields(object_name=object_name)
    cf_columns = [column["column_name"] for column in columns]
    array_columns = []
    array_values = []
    for column in payload:
        if column in cf_columns:
            array_columns.append(column)
            # TODO: Validation rules to field
            array_values.append(format_nulleable_values(payload[column]))

    if array:
        return array_columns, array_values

    str_columns = str_columns_parser(array_columns, append)
    str_values = str_columns_parser(array_values, append)

    return str_columns, str_values


def update_cf_columns(object_name: str, payload: dict, array: bool, append=False):
    columns = get_object_custom_fields(object_name=object_name)
    cf_columns = [column["column_name"] for column in columns]
    array_update = []
    for column in payload:
        if column in cf_columns:
            # TODO: Validation rules to field
            array_update.append(f"{column} = {format_nulleable_values(payload[column])}")

    if array:
        return array_update

    str_update = str_columns_parser(array_update, append)

    return str_update


def to_snake_format(input_string: str) -> str:
    formatted_string = input_string.replace(" ", "_").replace("-", "_")
    formatted_string = formatted_string.lower()
    return formatted_string


def dynamo_decimal_parse(items: dict) -> dict:
    for key, value in items.items():
        if isinstance(value, float):
            items[key] = Decimal(str(value))
    return items


def is_uuid_valid(value) -> bool:
    if value is None or value == "":
        return False
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True
