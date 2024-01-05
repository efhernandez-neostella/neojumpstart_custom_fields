import json
import os

import boto3
from custom_endpoint_utils import get_entity_unique_constraint

from core_utils.functions import REGION_NAME, rds_execute_statement
from sql_handler.sql_tables_handler import SQLTable, execute
from utils.common import (
    DecimalEncoder,
    dynamo_execute_statement,
    format_for_rds,
    parse_insert_data_rds,
    parse_update_data_rds,
)
from utils.exceptions.controller_exceptions import ControllerException

LIMIT_QUERY = 2001

FIELDS_TABLE = os.environ["FIELDS_TABLE"]
DYNAMO_RESOURCE = boto3.resource("dynamodb", region_name=REGION_NAME)
DYNAMO_CLIENT = boto3.client("dynamodb", region_name=REGION_NAME)
FIELDS = DYNAMO_RESOURCE.Table(FIELDS_TABLE)


class CustomObjectModel:
    def __init__(self, user_id: str, tenant_id: str, entity_name: str, entity_id: str = ""):
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.entity_name = entity_name
        self.entity_id = entity_id

    def get_custom_objects(self, conditionals: list = []) -> list:
        sql = SQLTable()
        table = self.entity_name
        left_join_query = "users_master um ON en.created_by = um.cognito_user_id"
        left_join_2_query = "users_master ums ON en.updated_by = ums.cognito_user_id"
        query_builder = (
            sql.select(
                f"{table} en",
                [
                    "en.*",
                    "um.full_name as crated_by_full_name ",
                    "um.cognito_user_id as created_by_id",
                    "ums.cognito_user_id as updated_by_id",
                    "ums.full_name as updated_by_full_name",
                ],
            )
            .left_join(left_join_query)
            .left_join(left_join_2_query)
        )

        if conditionals:
            query_builder = query_builder.where(conditionals)

        query = query_builder.limit(LIMIT_QUERY).query
        result = execute(query, table, False)
        return result

    def get_custom_object(self, conditional: list = []) -> list:
        sql = SQLTable()
        table = self.entity_name
        query = sql.select(table).where(conditional).query
        result = execute(query, table, False)
        return result[0] if len(result) > 0 else {}

    def get_custom_object_primary_key(self) -> str:
        sql = SQLTable()
        select_query = ["a.attname AS column_name"]
        table = "pg_index i"
        inner_join_query = "pg_attribute a ON a.attrelid = i.indrelid"
        where_query = [
            f"i.indrelid = '{self.entity_name}'::regclass",
            "a.attnum = ANY(i.indkey)",
            "i.indisprimary",
        ]
        query = (
            sql.select(table, select_query).inner_join(inner_join_query).where(where_query).query
        )
        return execute(query)[0]["column_name"]

    def get_custom_object_fields(self, select_query) -> str:
        sql = SQLTable()
        table = "information_schema.columns"
        where_query = ["table_schema = 'public'", f"table_name = '{self.entity_name}'"]
        query = sql.select(table, select_query).where(where_query).query
        return execute(query)

    def create_object(self, object_id, keys, insert_data):
        insert_data["created_by"] = self.user_id
        object_insert_data = {x: insert_data[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(object_insert_data)
        sql = SQLTable()
        table = self.entity_name
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value(f"{object_id}")
            .query
        )
        return execute(query)[0][f"{object_id}"]

    def update_object(self, object_id, id, keys, update_data):
        update_data["updated_by"] = self.user_id
        update_data["updated_at"] = "NOW()"
        object_update_data = {x: update_data[x] for x in keys}
        parsed_update_data = parse_update_data_rds(object_update_data)
        sql = SQLTable()
        table = self.entity_name
        where_query = [f"{object_id} = '{id}'"]
        query = (
            sql.update(table, parsed_update_data)
            .where(where_query)
            .return_value(f"{object_id}")
            .query
        )
        return execute(query)[0][f"{object_id}"]

    def check_if_already_exists(self, entity_name: str, payload: dict):
        unique_constraints = get_entity_unique_constraint(entity_name)
        if unique_constraints:
            conditional = []
            for constraint_data in unique_constraints:
                if column := constraint_data.get("column_name"):
                    payload_value = payload[f"{column}"]
                    conditional.append(f"{column} = '{payload_value}'")
            object = self.get_custom_object(conditional)
            if object:
                raise ControllerException(
                    409,
                    {
                        "conflict": f"Duplicate value detected ({column}).",
                        "code": "CustomObject.InvalidPayloadData",
                    },
                )

    def get_custom_fields(self) -> list:
        sql = SQLTable()
        table = "custom_fields_master"
        where_query = [f"parent_object_id = '{self.entity_id}'"]
        query = sql.select(table).where(where_query).query
        fields = execute(query)

        tenant_condition = f"WHERE \"tenant_id\" = '{self.tenant_id}'"
        parent_condition = f"AND \"parent_object_id\" = '{self.entity_id}'"
        dynamo_sql = f'SELECT * FROM "{FIELDS_TABLE}" {tenant_condition} {parent_condition}'
        dynamo_fields = dynamo_execute_statement(dynamo_sql)
        dynamo_fields = json.loads(json.dumps(dynamo_fields, cls=DecimalEncoder))

        for field in fields:
            field_intersection = list(
                filter(
                    lambda _field: _field["custom_field_id"] == field["custom_field_id"],
                    dynamo_fields,
                )
            )[0]
            field["properties"] = field_intersection.get("properties")

        return fields

    def get_entity_table_data(self, entity_id: str):
        select_query = [
            "em.database_name",
            "c.column_name AS database_entity_primary_key",
        ]
        table = "information_schema.key_column_usage c"
        left_join_query = "information_schema.table_constraints \
            t ON t.constraint_name = c.constraint_name"
        left_join_2_query = "entities_master em ON t.table_name = em.database_name"
        where_query = [
            f"em.entity_id = '{entity_id}'",
            "t.constraint_type = 'PRIMARY KEY'",
        ]
        query = (
            SQLTable()
            .select(table, select_query)
            .left_join(left_join_query)
            .left_join(left_join_2_query)
            .where(where_query)
            .query
        )
        return execute(query)[0]

    def get_property_schema(self, property_schemas_id: str) -> dict:
        sql = SQLTable()
        table = "property_schemas"
        where_query = [f"property_schemas_id = '{property_schemas_id}'"]
        query = sql.select(table).where(where_query).query
        return execute(query)[0]

    def get_linking_table(self, linking_tables_id: str):
        sql = SQLTable()
        table = "linking_tables"
        where_query = [f"linking_tables_id = '{linking_tables_id}'"]
        query = sql.select(table).where(where_query).query
        return execute(query)[0]["linking_table_name"]

    def create_linking_table_record(
        self, table: str, object_id: str, keys: list, insert_data: dict
    ):
        object_insert_data = {x: insert_data[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(object_insert_data)
        sql = SQLTable()
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value(f"{object_id}")
            .query
        )
        return execute(query)[0][f"{object_id}"]

    def update_linking_table_record(
        self, table: str, object_id: str, id: str, keys: list, update_data: dict
    ):
        record_update_data = {x: update_data[x] for x in keys}
        parsed_update_data = parse_update_data_rds(record_update_data)
        sql = SQLTable()
        where_query = [f"{object_id} = '{id}'"]
        query = (
            sql.update(table, parsed_update_data)
            .where(where_query)
            .return_value(f"{object_id}")
            .query
        )
        return execute(query)[0][f"{object_id}"]

    def delete_linking_table_record(self, table: str, column: str, ids: list):
        sql = SQLTable()
        values = [format_for_rds(id) for id in ids]
        query = sql.delete(table).where_in(column, values).query
        return rds_execute_statement(query)

    def get_table_records(self, table: str):
        sql = SQLTable()
        query = sql.select(table).query
        return execute(query)

    def get_field_data(
        self,
        db_name: str,
        ids: list,
        column_name_field_master: str,
        column_name_pk: str,
    ) -> list:
        sql = SQLTable()
        values = [format_for_rds(id) for id in ids]

        query = (
            sql.select(
                db_name,
                [
                    column_name_pk,
                    column_name_field_master,
                ],
            )
            .where_in(column_name_pk, values)
            .query
        )
        result = execute(query)
        return result

    def get_values(self, ids: list) -> list:
        """
        Get values from different tables that are going to help us to get the
        required field data.

        Args:
            ids (list): ids of property schemas

        Returns:
            list: The data to find values from different tables resources
        """
        values = [format_for_rds(id) for id in ids]
        table = "property_schemas"
        sql = SQLTable()
        query = (
            sql.select(
                "property_schemas ps",
                [
                    "ps.property_field_id",
                    "ps.property_entity_id",
                    "ps.is_multiple",
                    "ps.is_picklist",
                    "ps.property_schemas_id",
                    "em.entity_id",
                    "em.database_name",
                    "cfm.column_name AS custom_field_name",
                    """(SELECT a.attname FROM pg_index i
                    INNER JOIN pg_attribute a ON a.attrelid = i.indrelid
                    WHERE i.indrelid = em.database_name::regclass
                    AND a.attnum = ANY(i.indkey)
                    AND i.indisprimary) AS column_name_pk""",
                ],
            )
            .left_join("entities_master em ON ps.property_entity_id = em.entity_id")
            .left_join("custom_fields_master cfm ON ps.property_field_id = cfm.custom_field_id")
            .where_in("ps.property_schemas_id", values)
            .query
        )

        return execute(query, table, False)

    def get_picklist_data(self, ids: list) -> list:
        values = [format_for_rds(id) for id in ids]
        sql = SQLTable()
        table = "picklist_master"

        query = sql.select(table, ["picklist_id", "name"]).where_in("picklist_id", values).query

        return execute(query)
