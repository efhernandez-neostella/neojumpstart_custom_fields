import base64
import json
import os
import sys
import traceback
import uuid
from datetime import datetime
from threading import Thread
from urllib import response

import boto3
import requests

from utils.constants import HEADERS
from utils.exceptions.controller_exceptions import ControllerException
from utils.exceptions.server_exceptions import error_handler

env_path = "aws_data.env"
if os.path.exists(env_path):
    __import__("dotenv").load_dotenv(env_path, override=True)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", None)
    profile_name = os.getenv("PROFILE", None)
    REGION_NAME = os.getenv("REGION_NAME", None)
    if AWS_ACCESS_KEY_ID is not None:
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
        boto3.setup_default_session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=REGION_NAME,
        )
    else:
        boto3.setup_default_session(profile_name=profile_name, region_name=REGION_NAME)


def get_secret(vault_name, region_name, secret_name) -> dict:
    # This functions retrieves secrets from AWS Secrets Manager
    secrets_client = boto3.client(service_name="secretsmanager", region_name=region_name)
    get_secret_value_response = secrets_client.get_secret_value(SecretId=vault_name)
    secrets = json.loads(get_secret_value_response["SecretString"])
    if secret_name:
        return secrets[secret_name]
    else:
        return secrets


def _get_coralogix_secrets() -> tuple:
    coralogix_data = get_secret(os.getenv("CORALOGIX_SECRET", None), REGION_NAME, None)
    return coralogix_data.get("CoralogixKey", None), coralogix_data.get("CoralogixUrl", None)


SERVICE_NAME = os.getenv("SERVICE_NAME", None)
RESOURCE_METHOD = os.getenv("RESOURCE_METHOD", None)
REGION_NAME = os.getenv("REGION_NAME", None)
COGNITO_CLIENT = boto3.client("cognito-idp", region_name=REGION_NAME)
RDS_CLIENT = boto3.client("rds-data", region_name=REGION_NAME)
LAMBDA_CLIENT = boto3.client("lambda", region_name=REGION_NAME)
DATABASE_NAME = os.getenv("DATABASE_NAME", None)
DB_CLUSTER_ARN = os.getenv("DB_CLUSTER_ARN", None)
DB_CREDENTIALS_SECRETS_STORE_ARN = os.getenv("DB_CREDENTIALS_SECRETS_STORE_ARN", None)
CORALOGIX_KEY, CORALOGIX_URL = _get_coralogix_secrets()
APPKEY_SECRET_ARN = os.getenv("APPKEY_SECRET_ARN", None)
TRANSACTION_ID = None
THREADS = []
UUID = uuid.uuid4().hex
CURRENT_DATETIME = datetime.now()
TRANSACTION_ID = None


def initialize():
    global UUID, CURRENT_DATETIME
    UUID = uuid.uuid4().hex
    CURRENT_DATETIME = datetime.now()
    return UUID, CURRENT_DATETIME


def sendCoralogix(
    private_key,
    logs,
    app_name,
    subsystem_name,
    severity,
    computer_name=None,
    class_name=None,
    category=None,
    method_name=None,
):
    """
    This function sends a request to Coralogix with the given data.
    private_key: Coralogix account private key, as String.
    logs: the logs text, as String.
    app_name: Application Name to be shown in Coralogix, as String.
    subsystem_name: Subsystem Name to be shown in Coralogix, as String.
    severity: Severity of the logs as String. Values: 1 – Debug, 2 – Verbose, 3 – Info, 4 – Warn,
    5 – Error, 6 – Critical
    computer_name: Computer Name to be shown in Coralogix, as String.
    class_name: Class Name to be shown in Coralogix, as String.
    category: Category to be shown in Coralogix, as String.
    method_name: Method Name to be shown in Coralogix, as String.
    """

    url = CORALOGIX_URL
    # Get the datetime and change it to miliseconds
    now = datetime.now()

    data = {
        "privateKey": private_key,
        "applicationName": app_name,
        "subsystemName": subsystem_name,
        "logEntries": [
            {
                "timestamp": now.timestamp() * 1000,  # 1457827957703.342,
                "text": logs,
                "severity": severity,
            }
        ],
    }
    if computer_name:
        data["computerName"] = computer_name
    if class_name:
        data["logEntries"][0]["className"] = class_name
    if category:
        data["logEntries"][0]["category"] = category
    if method_name:
        data["logEntries"][0]["methodName"] = method_name

    # Make the request to coralogix
    requests.post(url, json=data)

    return True


def send_to_coralogix(
    private_key,
    logs,
    app_name,
    subsystem_name,
    severity,
    computer_name=None,
    class_name=None,
    category=None,
    method_name=None,
):
    global THREADS
    thread = Thread(
        target=sendCoralogix,
        args=(
            private_key,
            logs,
            app_name,
            subsystem_name,
            severity,
            computer_name,
            class_name,
            category,
            method_name,
        ),
    )
    THREADS.append(thread)
    thread.start()


def create_transaction(database=None, resourceArn=None, secretArn=None):
    if None in [database, resourceArn, secretArn]:
        database, resourceArn, secretArn = (
            DATABASE_NAME,
            DB_CLUSTER_ARN,
            DB_CREDENTIALS_SECRETS_STORE_ARN,
        )
    params = {"secretArn": secretArn, "database": database, "resourceArn": resourceArn}

    response = RDS_CLIENT.begin_transaction(**params)

    if database == DATABASE_NAME:
        globals()["TRANSACTION_ID"] = response["transactionId"]
    return response["transactionId"]


def confirm_transaction(resourceArn=None, secretArn=None, transactionId=None):
    if None in [resourceArn, secretArn]:
        resourceArn, secretArn = DB_CLUSTER_ARN, DB_CREDENTIALS_SECRETS_STORE_ARN
    if transactionId is None:
        transactionId = globals()["TRANSACTION_ID"]
    if resourceArn == DB_CLUSTER_ARN:
        globals()["TRANSACTION_ID"] = None

    RDS_CLIENT.commit_transaction(
        resourceArn=resourceArn, secretArn=secretArn, transactionId=transactionId
    )


def delete_transaction(resourceArn=None, secretArn=None, transactionId=None):
    if None in [resourceArn, secretArn]:
        resourceArn, secretArn = DB_CLUSTER_ARN, DB_CREDENTIALS_SECRETS_STORE_ARN
    if transactionId is None:
        transactionId = globals()["TRANSACTION_ID"]
    if resourceArn == DB_CLUSTER_ARN:
        globals()["TRANSACTION_ID"] = None
    RDS_CLIENT.rollback_transaction(
        resourceArn=resourceArn, secretArn=secretArn, transactionId=transactionId
    )


def rds_execute_statement(sql, database=None, resourceArn=None, secretArn=None, transactionId=None):
    if None in [database, resourceArn, secretArn]:
        database, resourceArn, secretArn = (
            DATABASE_NAME,
            DB_CLUSTER_ARN,
            DB_CREDENTIALS_SECRETS_STORE_ARN,
        )

    # Execute sql statement through boto3
    if "UUID" in globals():
        send_to_coralogix(
            CORALOGIX_KEY,
            {"correlation_id": UUID, "Query string": sql},
            SERVICE_NAME,
            RESOURCE_METHOD,
            3,
        )
    params = {
        "secretArn": secretArn,
        "database": database,
        "resourceArn": resourceArn,
        "sql": sql,
        "includeResultMetadata": True,
    }
    if transactionId is not None:
        params["transactionId"] = transactionId
    elif globals()["TRANSACTION_ID"] is not None and database == DATABASE_NAME:
        params["transactionId"] = globals()["TRANSACTION_ID"]
    response = RDS_CLIENT.execute_statement(**params)
    return response


def deserialize_rds_response(data):
    """
    function to return a dictionary of attribute-value of RDS response in boto3
    """
    records = data["records"]
    columns = data["columnMetadata"]
    result = []
    for record in records:
        record_dict = {}
        col_count = 0
        for current_column in columns:
            attribute = current_column["name"]
            type_name = current_column["typeName"]
            key = list(record[col_count].keys())[0]
            value = list(record[col_count].values())[0]
            if type_name in ("numeric") and key != "isNull":
                record_dict[attribute] = float(value)
            elif key in ["booleanValue", "doubleValue", "longValue"]:
                record_dict[attribute] = value
            elif key == "stringValue":
                # Check if it is a json as a string or only a string
                try:
                    json_value = json.loads(value)
                    record_dict[attribute] = (
                        str(json_value)
                        if (
                            isinstance(json_value, int) or isinstance(json_value, float)
                        )
                        else json_value
                    )
                except json.JSONDecodeError:
                    record_dict[attribute] = str(value)
            elif key == "arrayValue":
                record_dict[attribute] = list(value.values())[0]
            elif key == "isNull":
                record_dict[attribute] = None
            # TODO check blobValue return
            elif key == "blobValue":
                record_dict[attribute] = value
            col_count += 1

        result.append(record_dict)

    return result


def set_format_to_response(field: str) -> str:
    array_words = field.split("_")
    return "".join([word.capitalize() for word in array_words])


def get_tenant_id(user_id):
    sql = f"SELECT tenant_id FROM users_master WHERE cognito_user_id ='{user_id}'"
    rds_response = rds_execute_statement(sql)
    return rds_response["records"][0][0]["stringValue"]


def get_pool_id(tenant_id):
    sql = f"SELECT user_pool_id FROM tenants_master WHERE tenant_id = '{tenant_id}'"
    rds_response = rds_execute_statement(sql)
    return rds_response["records"][0][0]["stringValue"]


def check_tenant_level_permissions(tenant_id, module, component, subcomponent):
    sql = (
        f"SELECT COUNT(*) from tenant_permissions INNER JOIN components_master ON "
        f"tenant_permissions.components_id = components_master.components_id "
        f"WHERE tenant_permissions.tenant_id = '{tenant_id}' AND "
        f"components_master.module = '{module}' AND "
        f"components_master.component = '{component}' AND "
        f"components_master.subcomponent ='{subcomponent}' AND "
        f"components_master.is_active = true "
    )
    count = rds_execute_statement(sql)["records"][0][0]["longValue"]
    if count == 0:
        return False
    else:
        return True


def table_exists(_schema, _table):
    """Check whether the table 'license_roles' exists, and that the current user has access to it.
    Args:
        _schema (str): schema name where the table resides.
        _table (str): the table name that you want to check exists.
    Returns:
        boolean: True if schema.table exists, else False.
    """
    sql = (
        "SELECT EXISTS ( "
        "SELECT FROM information_schema.tables "
        f"WHERE  table_schema = '{_schema}' "
        f"AND    table_name   = '{_table}'"
        ");"
    )
    resp = rds_execute_statement(sql)["records"][0][0]["booleanValue"]

    if isinstance(resp, str):
        if resp.lower() == "true":
            resp = True
        else:
            resp = False

    return resp


def check_user_level_permissions(tenant_id, user_id, module, component, subcomponent, action):
    if (USER_AGENT == "Tenant Key") or (USER_AGENT == "Application Key"):
        return True
    sql = (
        f"SELECT COUNT(*) from role_permissions INNER JOIN components_master ON "
        f"role_permissions.components_id = components_master.components_id "
        f"WHERE role_permissions.tenant_id = '{tenant_id}' "
        f"AND components_master.module = '{module}' "
        f"AND components_master.component = '{component}' "
        f"AND components_master.subcomponent ='{subcomponent}' "
        f"AND role_permissions.{action} = TRUE "
        f"AND components_master.is_active = true "
        f"AND role_permissions.role_id IN "
        f"("
        f"SELECT role_id from user_roles WHERE cognito_user_id = '{user_id}'"
        f")"
    )
    count = rds_execute_statement(sql)["records"][0][0]["longValue"]

    # Check whether the table 'license_roles' exists, and that the current user has access to it.
    if table_exists("public", "license_roles"):
        sql_stripe = f"""SELECT COUNT(*) FROM role_permissions 
            INNER JOIN components_master ON 
            role_permissions.components_id = components_master.components_id 
            WHERE role_permissions.tenant_id = '{tenant_id}' 
            AND components_master.module = '{module}' 
            AND components_master.component = '{component}' 
            AND components_master.subcomponent = '{subcomponent}' 
            AND role_permissions.{action} = TRUE 
            AND components_master.is_active = TRUE 
            AND role_permissions.role_id IN 
            (
            SELECT license_roles.role_id FROM license_roles 
            INNER JOIN user_licenses ON user_licenses.license_id = license_roles.license_id 
            WHERE user_licenses.cognito_user_id = '{user_id}'
            )"""

        count_stripe = rds_execute_statement(sql_stripe)["records"][0][0]["longValue"]
    else:
        count_stripe = 0

    count += count_stripe

    if count == 0:
        return False
    else:
        return True


def check_tenant_limit(table, tenant_id):
    limit_query = f"""SELECT object_limit FROM objects_master WHERE table_name = '{table}' 
        AND tenant_id = '{tenant_id}'"""
    limit = rds_execute_statement(limit_query)["records"][0][0]["longValue"]
    user_len_query = (
        f"SELECT COUNT(*) from {table} WHERE tenant_id = '{tenant_id}' AND is_active = true"
    )
    user_len = rds_execute_statement(user_len_query)["records"][0][0]["longValue"]
    if limit == 0:
        return True
    if limit <= user_len:
        return False
    else:
        return True


def decode_key(key):
    # This function extracts the tenant_id from the Tenant Key
    encoded_bytes = bytes(key, "utf-8")
    decoded_str = str(base64.b64decode(encoded_bytes.decode("utf-8")), "utf-8")
    tenant_id_from_key = decoded_str.split(":")[0]
    return tenant_id_from_key


def get_account_id(user_id):
    sql = f"SELECT account_id FROM users_master WHERE cognito_user_id = '{user_id}' "
    rds_response = rds_execute_statement(sql)
    user_data = deserialize_rds_response(rds_response)
    if len(user_data):
        return user_data[0]["account_id"]
    else:
        return None


def get_unit_test_user():
    global USER_AGENT
    USER_AGENT = "Application Key"
    sql = "SELECT cognito_user_id FROM users_master WHERE email = 'App-Key'"
    cognito_user_id = deserialize_rds_response(rds_execute_statement(sql))[0]["cognito_user_id"]
    return cognito_user_id


def check_api_keys(event):
    # This function determines the user_id and tenant_id according to the api key used
    if "USER_AGENT" not in globals():
        global USER_AGENT
        USER_AGENT = "User"
    if event["requestContext"]["authorizer"]["claims"]["scope"] == "aws.cognito.signin.user.admin":
        user_id = event["requestContext"]["authorizer"]["claims"]["username"]
        tenant_id = get_tenant_id(user_id)
        return (user_id, tenant_id)
    elif event["requestContext"]["authorizer"]["claims"]["scope"] == "apiauthidentifier/json.read":
        if "Tenant-Key" in event["headers"].keys():
            USER_AGENT = "Tenant Key"
            tenant_key = event["headers"]["Tenant-Key"]
            tenant_id = decode_key(tenant_key)
            # Get vault names
            sql = f"SELECT secret_name from tenant_keys WHERE tenant_id = '{tenant_id}'"
            response = rds_execute_statement(sql)
            if len(response["records"]) == 0:
                raise Exception("api_keys.ApiKeyNotFoundInHeaders")
            # Compare key with secrets
            for curr_vault in response["records"]:
                curr_secret = get_secret(curr_vault[0]["stringValue"], REGION_NAME, "Key")
                if curr_secret == tenant_key:
                    sql = f"""SELECT cognito_user_id FROM users_master 
                        WHERE tenant_id = '{tenant_id}' 
                        AND first_name = '{curr_vault[0]['stringValue']}' AND 
                        email = '{curr_vault[0]['stringValue']}'"""
                    response = deserialize_rds_response(rds_execute_statement(sql))
                    if len(response) == 0:
                        sql = f"""SELECT cognito_user_id FROM users_master 
                            WHERE tenant_id = '{tenant_id}' 
                            AND first_name = 'Tenant' AND last_name = 'Key'"""
                        response = deserialize_rds_response(rds_execute_statement(sql))
                        user_id = response[0]["cognito_user_id"]
                    else:
                        user_id = response[0]["cognito_user_id"]
                    return (user_id, tenant_id)
            raise Exception("api_keys.InvalidTenantKey")
        elif "App-Key" in event["headers"].keys():
            USER_AGENT = "Application Key"
            tenant_id = event["headers"]["tenant_id"]
            app_key = event["headers"]["App-Key"]
            sql = f"""SELECT cognito_user_id FROM users_master WHERE tenant_id = '{tenant_id}' 
            AND first_name = 'App' AND last_name = 'Key'"""
            user_id = rds_execute_statement(sql)["records"][0][0]["stringValue"]
            # Compare key with secrets
            secrets_client = boto3.client(service_name="secretsmanager", region_name=REGION_NAME)
            secret_response = secrets_client.get_secret_value(SecretId=APPKEY_SECRET_ARN)
            secret_key = secret_response["SecretString"]
            if secret_key == app_key:
                return (user_id, tenant_id)
            else:
                raise Exception("api_keys.InvalidApplicationKey")
        else:
            raise Exception("api_keys.ApiKeyNotFoundInHeaders")
    else:
        raise Exception("api_keys.ScopeNotSupported")


def throttling_check():
    return False


def wait_for_threads():
    for thread in THREADS:
        thread.join()


def send_sns_message(message):
    SNS_CLIENT = boto3.client("sns", region_name=REGION_NAME)
    SNS_ARN = os.getenv("SNS_ARN", None)
    sns_message = {"default": json.dumps(message)}
    response = SNS_CLIENT.publish(
        TargetArn=SNS_ARN, Message=json.dumps(sns_message), MessageStructure="json"
    )
    send_to_coralogix(
        CORALOGIX_KEY,
        {"UUID": UUID, "SNS Body": sns_message, "Response": response},
        SERVICE_NAME,
        RESOURCE_METHOD,
        3,
    )
    return response


def get_object_id(object_name, tenant_id):
    """
    Function to return the object id
    """
    sql = f"""SELECT object_id FROM objects_master WHERE table_name = '{object_name}' 
                and tenant_id = '{tenant_id}'"""
    rds_response = rds_execute_statement(sql)
    object_data = deserialize_rds_response(rds_response)
    if len(object_data) > 0:
        return object_data[0]["object_id"]
    else:
        raise f"{object_name} object doesn't exists"


def get_table_columns(object_name):
    sql = f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{object_name}'
    """

    column_names = deserialize_rds_response(rds_execute_statement(sql))

    columns_query = []

    for column in column_names:
        column_name = column["column_name"]

        # Get created_by or updated_by full name
        if column_name in ["created_by", "updated_by"]:
            column_name = f"""
                {column_name} AS {column_name}_id, 
                    (SELECT full_name FROM users_master 
                    WHERE cognito_user_id = o.{column_name}) as {column_name}
            """

        columns_query.append(column_name)

    columns_query = ", ".join(columns_query)

    return columns_query


def get_object_properties(object_name, primary_key_name, primary_key_value):
    """
    Function to return all properties of an object
    """
    columns = get_table_columns(object_name)
    sql = f"SELECT {columns} FROM {object_name} o WHERE {primary_key_name} = '{primary_key_value}'"
    rds_response = rds_execute_statement(sql)
    object_data = deserialize_rds_response(rds_response)
    if len(object_data) > 0:
        return object_data[0]
    else:
        raise f"{object_name} object with {primary_key_value} as primary key doesn't exists"


def get_fks(object_name):
    """
    return fk table columns data
    """

    # GET FK columns and table names
    sql = f"""
        select 
            att2.attname as "child_column", 
            cl.relname as "parent_table", 
            att.attname as "parent_column",
            conname
        from
        (select 
                unnest(con1.conkey) as "parent", 
                unnest(con1.confkey) as "child", 
                con1.confrelid, 
                con1.conrelid,
                con1.conname
            from 
                pg_class cl
                join pg_namespace ns on cl.relnamespace = ns.oid
                join pg_constraint con1 on con1.conrelid = cl.oid
            where
                cl.relname = '{object_name}'
                and con1.contype = 'f'
        ) con
        join pg_attribute att on
            att.attrelid = con.confrelid and att.attnum = con.child
        join pg_class cl on
            cl.oid = con.confrelid
        join pg_attribute att2 on
            att2.attrelid = con.conrelid and att2.attnum = con.parent
    """

    rds_response = rds_execute_statement(sql)
    fks_data = deserialize_rds_response(rds_response)

    return fks_data


def get_object_children(object_name, properties, tenant_id, primary_key_value, primary_key_name):
    """
    return children tables data
    """
    # Check if table is parent_table
    sql = f"""SELECT table_name FROM objects_master 
        WHERE parent_object IN 
        (SELECT object_id FROM objects_master 
            WHERE table_name = '{object_name}' AND 
            tenant_id = '{tenant_id}'
        )
        """
    rds_response = rds_execute_statement(sql)
    children_data = deserialize_rds_response(rds_response)

    for child in children_data:
        # Get FKs from child
        fks_data = get_fks(child["table_name"])

        child_fk_data = [
            fk
            for fk in fks_data
            if fk["parent_table"] == object_name and fk["child_column"] not in ["tenant_id"]
        ]
        child_fk_data = child_fk_data[0] if len(child_fk_data) else {}
        if child_fk_data:
            child_fk_column = child_fk_data["child_column"]
            # Query columns of tables
            sql = f"""SELECT * FROM {child["table_name"]} 
                WHERE {child_fk_column} = '{primary_key_value}'"""
            child_data = deserialize_rds_response(rds_execute_statement(sql))
            properties[child["table_name"]] = child_data

    return properties


def get_object_fk(object_name, properties):
    fks_data = get_fks(object_name)
    # Change FK columns values to object attribute
    for fk in fks_data:
        if fk["child_column"] in properties:
            # Check not null value on FK
            if properties[fk["child_column"]] is not None and fk["child_column"] not in [
                "tenant_id"
            ]:
                if fk["child_column"] not in ["created_by", "updated_by"]:
                    properties[fk["child_column"]] = get_object_properties(
                        fk["parent_table"], fk["parent_column"], properties[fk["child_column"]]
                    )
                elif fk["child_column"] == "created_by":
                    properties[fk["child_column"]] = get_object_properties(
                        fk["parent_table"], fk["parent_column"], properties["created_by_id"]
                    )
                elif fk["child_column"] == "updated_by":
                    properties[fk["child_column"]] = get_object_properties(
                        fk["parent_table"], fk["parent_column"], properties["updated_by_id"]
                    )
    return properties


def get_object_primary_key(object_name):
    """
    Function to return the name of object primary key
    """

    sql = f"""SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{object_name}'::regclass
                AND i.indisprimary;
            """

    data = deserialize_rds_response(rds_execute_statement(sql))

    # Table primary key
    return data[0]["attname"]


def check_linking_table(object_name, tenant_id):
    sql = f"""SELECT linking_table FROM 
        objects_master WHERE table_name = '{object_name}' 
        AND tenant_id = '{tenant_id}'
    """
    parent_data = deserialize_rds_response(rds_execute_statement(sql))

    linking_table = parent_data[0]["linking_table"] if len(parent_data) else False

    return linking_table


def check_if_child(object_name, tenant_id, event_body):
    """
    Function to check if object is a child and change the event data to show from the father
    """
    # Get FKs
    fk_data = get_fks(object_name)

    if not len(fk_data):
        return object_name, event_body, object_name

    # Check if the object has parent
    sql = f"""SELECT parent_object FROM 
        objects_master WHERE table_name = '{object_name}' 
        AND tenant_id = '{tenant_id}'
    """
    parent_data = deserialize_rds_response(rds_execute_statement(sql))

    if not len(parent_data):
        return object_name, event_body, object_name

    # Get parent object id
    parent_object_id = parent_data[0]["parent_object"]

    if not parent_object_id:
        return object_name, event_body, object_name

    # Return parent object name
    sql = f"""SELECT table_name FROM 
        objects_master WHERE object_id = '{parent_object_id}'
    """

    parent_table = deserialize_rds_response(rds_execute_statement(sql))[0]["table_name"]

    # Check parent columns data
    fk_key = ""
    fk_parent_id_column = ""

    for fk in fk_data:
        if parent_table == fk["parent_table"]:
            fk_key = fk["child_column"]
            fk_parent_id_column = fk["parent_column"]
            break

    # If not fk data with parent, return normal values
    if not fk_parent_id_column or not fk_key:
        return object_name, event_body, object_name

    # Change fk column attribute to pk from parent
    event_body[fk_parent_id_column] = event_body.pop(fk_key, None)

    return parent_table, event_body, object_name


def webhook_dispatch(object_name, action):
    """
    Decorator to handle a webhook
    """

    def decorator(lambda_function):
        def wrapper(*args, **kwargs):
            if "test" in args[0] or os.getenv("SNS_ARN", None) is None:
                return lambda_function(*args, **kwargs)
            event = args[0]
            if "user_id" in event and "tenant_id" in event:
                user_id, tenant_id = event["user_id"], event["tenant_id"]
            else:
                user_id, tenant_id = check_api_keys(event)
            event_body = json.loads(event["body"])
            # Get new data from parents if the object has any
            webhook_object_name, event_body, aux_object_name = check_if_child(
                object_name, tenant_id, event_body
            )
            aux_primary_key_name = get_object_primary_key(aux_object_name)
            primary_key_name = get_object_primary_key(webhook_object_name)
            primary_key_value = ""

            # get old properties (only if action is update).
            previous_properties = {}
            if action == "update" or webhook_object_name != aux_object_name:
                # if the primary key doesn't exists in the event_body,
                # then the lambda function will run without webhook
                if not (primary_key_name in event_body):
                    return lambda_function(*args, **kwargs)
                primary_key_value = event_body[primary_key_name]
                previous_properties = get_object_properties(
                    webhook_object_name, primary_key_name, primary_key_value
                )

                # Get FK properties
                previous_properties = get_object_fk(webhook_object_name, previous_properties)

                # Get Children properties
                previous_properties = get_object_children(
                    webhook_object_name,
                    previous_properties,
                    tenant_id,
                    primary_key_value,
                    primary_key_name,
                )

            # excecute lambda function
            response = lambda_function(*args, **kwargs)
            response_status = response["statusCode"]
            response_body = json.loads(response["body"])

            # Send the webhook event to sns
            if response_status == 200 and aux_primary_key_name in response_body:
                primary_key_value = (
                    response_body[primary_key_name]
                    if aux_primary_key_name == primary_key_name
                    else event_body[primary_key_name]
                )
                # get new properties
                properties = get_object_properties(
                    webhook_object_name, primary_key_name, primary_key_value
                )

                # Get FK properties
                properties = get_object_fk(webhook_object_name, properties)

                # Get Children properties
                properties = get_object_children(
                    webhook_object_name, properties, tenant_id, primary_key_value, primary_key_name
                )

                # get object id
                object_id = get_object_id(webhook_object_name, tenant_id)

                # Process uuid
                uuid_value = ""
                if response_body.get("UUID"):
                    uuid_value = response_body.get("UUID")
                elif response_body.get("correlation_id"):
                    uuid_value = response_body.get("correlation_id")
                date = str(datetime.now())

                # Webhook event topic
                send_sns_message(
                    {
                        "tenant_id": tenant_id,
                        "object_id": object_id,
                        "record_id": primary_key_value,
                        # change action to update if it is a child project
                        "action": "update"
                        if check_linking_table(aux_object_name, tenant_id)
                        else action,
                        "system_event_id": uuid_value,
                        "user_id": user_id,
                        "date": date,
                        "properties": properties,
                        "previous_properties": previous_properties,
                    }
                )

            return response  # return lambda function response

        return wrapper

    return decorator


def get_lambda_versions_list(lambda_arn):
    versions_list = []
    next_marker = ""
    while True:
        if next_marker == "":
            lambda_response = LAMBDA_CLIENT.list_versions_by_function(FunctionName=lambda_arn)
        else:
            lambda_response = LAMBDA_CLIENT.list_versions_by_function(
                FunctionName=lambda_arn, Marker=next_marker
            )
        versions_list = versions_list + [
            int(curr_version["Version"])
            for curr_version in lambda_response["Versions"]
            if curr_version["Version"] != "$LATEST"
        ]
        if "NextMarker" not in lambda_response:
            break
        else:
            next_marker = lambda_response["NextMarker"]
    versions_list.sort()
    return versions_list


def invoke_requested_version(lambda_arn, event):
    # Check if requested version is valid
    requested_version = int(event["headers"]["version"])
    if not isinstance(requested_version, int):
        raise Exception("api_versioning.VersionMustBeANegativeInteger")
    if requested_version >= 0:
        raise Exception("api_versioning.VersionMustBeANegativeInteger")

    # Retrieve versions list
    requested_version = requested_version - 1
    versions_list = get_lambda_versions_list(lambda_arn)
    if abs(requested_version) > len(versions_list):
        raise Exception("api_versioning.VersionDoesNotExist")
    version_to_execute = versions_list[requested_version]

    # Execute requested version
    event["headers"].pop("version")
    response = LAMBDA_CLIENT.invoke(
        FunctionName=lambda_arn + ":" + str(version_to_execute),
        InvocationType="RequestResponse",
        Payload=json.dumps(event),
    )
    payload = json.loads(response["Payload"].read())
    return payload


def format_string_rds(s):
    return rf"{s}".replace("\\", r"\\").replace("'", r"\'").replace("\n", r"\\n")


def get_db_credentials(tenant_id: str = None):
    sql = f"""SELECT * FROM tenants_master where tenant_id = '{tenant_id}'"""
    tenants = deserialize_rds_response(rds_execute_statement(sql))
    if len(tenants) == 0:
        raise Exception("tenant_configuration.TenantNotFound")
    tenant = tenants[0]
    database, resourceArn, secretArn = None, None, None

    # Use tenant rds credentials
    if (
        "database_name" in tenant
        and "db_cluster_arn" in tenant
        and "db_credentials_secrets_store_arn" in tenant
    ):
        database, resourceArn, secretArn = (
            tenant["database_name"],
            tenant["db_cluster_arn"],
            tenant["db_credentials_secrets_store_arn"],
        )

    # Use default credentials in case of null tenant credentials
    if database is None or resourceArn is None or secretArn is None:
        database, resourceArn, secretArn = (
            DATABASE_NAME,
            DB_CLUSTER_ARN,
            DB_CREDENTIALS_SECRETS_STORE_ARN,
        )

    return database, resourceArn, secretArn


def set_credentials(
    database=os.getenv("DATABASE_NAME", None),
    resourceArn=os.getenv("DB_CLUSTER_ARN", None),
    secretArn=os.getenv("DB_CREDENTIALS_SECRETS_STORE_ARN", None),
):
    globals()["DATABASE_NAME"] = database
    globals()["DB_CLUSTER_ARN"] = resourceArn
    globals()["DB_CREDENTIALS_SECRETS_STORE_ARN"] = secretArn


def tenant_setup(fn):
    def wrapper(*args, **kwargs):
        # Get tenant_id
        tenant_id = None
        event = args[0]
        set_credentials()
        if (
            event["requestContext"]["authorizer"]["claims"]["scope"]
            == "aws.cognito.signin.user.admin"
        ):
            user_id = event["requestContext"]["authorizer"]["claims"]["username"]
            tenant_id = get_tenant_id(user_id)
        elif (
            event["requestContext"]["authorizer"]["claims"]["scope"]
            == "apiauthidentifier/json.read"
        ):
            if "Tenant-Key" in event["headers"].keys():
                tenant_key = event["headers"]["Tenant-Key"]
                tenant_id = decode_key(tenant_key)
            elif "App-Key" in event["headers"].keys():
                tenant_id = event["headers"]["tenant_id"]
            else:
                raise Exception("tenant_configuration.InvalidApplicationKey")
        else:
            raise Exception("tenant_configuration.ScopeNotSupported")
        database, resourceArn, secretArn = get_db_credentials(tenant_id)
        # Set DB credentials for specific tenant_id
        set_credentials(database, resourceArn, secretArn)

        event["user_id"], event["tenant_id"] = check_api_keys(event)
        response = fn(*args, **kwargs)

        return response

    return wrapper


def lambda_decorator(fn):
    """
    fn: it can be a lambda function or a webhook_dispatch decorator
    """

    def wrapper(*args, **kwargs):
        try:
            event = args[0]
            context = args[1]
            if "version" in event["headers"]:
                LAMBDA_ARN = context.invoked_function_arn
                response = invoke_requested_version(LAMBDA_ARN, event)
                return response

            send_to_coralogix(
                CORALOGIX_KEY,
                {"correlation_id": UUID, "Event Received": event},
                SERVICE_NAME,
                RESOURCE_METHOD,
                3,
            )

            create_transaction()
            response = fn(*args, **kwargs)
            if "test" in event or response[0] != 200:
                delete_transaction()
            else:
                confirm_transaction()

            EXECUTION_TIME = str(datetime.now() - CURRENT_DATETIME)
            send_to_coralogix(
                CORALOGIX_KEY,
                {"correlation_id": UUID, "Execution time": EXECUTION_TIME, "response": response[1]},
                SERVICE_NAME,
                RESOURCE_METHOD,
                3,
            )
            wait_for_threads()

            return {"statusCode": response[0], "body": json.dumps(response[1]), "headers": HEADERS}
        except ControllerException as controller_exception:
            exception_response = controller_exception.response
            exception_response["headers"] = HEADERS
            delete_transaction()
            return exception_response
        except Exception as error:
            exc_type, _, exc_tb = sys.exc_info()
            print(traceback.format_exc())
            ERROR_MSG = f"Execution failed: {repr(error)}. Line: {str(exc_tb.tb_lineno)}."
            EXECUTION_TIME = str(datetime.now() - CURRENT_DATETIME)
            # Send error message and status to coralogix
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
            error_response = error_handler(exc_type, ERROR_MSG, UUID)
            error_response["headers"] = HEADERS
            delete_transaction()
            return error_response

    return wrapper
