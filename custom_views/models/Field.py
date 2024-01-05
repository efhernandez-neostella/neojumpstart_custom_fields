import json
import os

import boto3

from core_utils.functions import (
    REGION_NAME,
    deserialize_rds_response,
    rds_execute_statement,
)
from sql_handler.sql_tables_handler import SQLTable, execute
from sql_handler.sql_views_handler import SQLView
from utils.common import DecimalEncoder
from utils.exceptions.controller_exceptions import ControllerException

FIELDS_TABLE = os.environ["FIELDS_TABLE"]
DYNAMO_RESOURCE = boto3.resource("dynamodb", region_name=REGION_NAME)
DYNAMO_CLIENT = boto3.client("dynamodb", region_name=REGION_NAME)
FIELDS = DYNAMO_RESOURCE.Table(FIELDS_TABLE)


class FieldModel:
    def __init__(self, custom_field_id: str, user_id: str = None, tenant_id: str = None):
        self.table: str = "custom_fields_master"
        self.user_id: str = user_id
        self.tenant_id: str = tenant_id
        self.custom_field_id: str = custom_field_id

    def get_field(self):
        dynamo_field = FIELDS.get_item(
            Key={"custom_field_id": self.custom_field_id, "tenant_id": self.tenant_id}
        )
        if "Item" in dynamo_field:
            dynamo_field = json.loads(json.dumps(dynamo_field["Item"], cls=DecimalEncoder))
            sql = (
                SQLView()
                .select(
                    f"""
                c.custom_field_id, c.is_active, c.column_name, c.name,
                c.parent_object_id, o.name as parent_object_name, c.input, c.data_type, c.format,
                c.is_required, c.description, c.width, c.height, c.data_source,
                c.tenant_id
                FROM {self.table} c
            """
                )
                .left_join("entities_master o ON o.entity_id = c.parent_object_id")
                .where(f"c.custom_field_id = '{self.custom_field_id}'")
                .query
            )
            rds_field = deserialize_rds_response(rds_execute_statement(sql))
            field = {**rds_field[0], "properties": dynamo_field.get("properties", {})}
            return field
        else:
            raise ControllerException(
                400,
                {
                    "result": "Field doesn't exist for this tenant",
                    "code": "Field.FieldDoesNotExist",
                },
            )

    def get_picklist_values(self):
        table = "picklist_master"
        select_query = ["picklist_id", "name", "is_active"]
        where_query = [f"custom_field_id = '{self.custom_field_id}'"]
        query = (
            SQLTable().select(table, select_query).where(where_query).order_by("order_num").query
        )
        return execute(query, table, False)

    def get_field_column_name(self, custom_field_id: str) -> list:
        sql = (
            SQLView()
            .select(f"""c.custom_field_id, c.column_name FROM {self.table} c""")
            .where(f"c.custom_field_id = '{custom_field_id}'")
            .query
        )
        rds_field = deserialize_rds_response(rds_execute_statement(sql))
        return rds_field[0]['column_name']
