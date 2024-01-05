import json
import os

import boto3

from core_utils.functions import REGION_NAME, deserialize_rds_response, rds_execute_statement
from sql_handler.sql_tables_handler import SQLTable, execute
from utils.common import DecimalEncoder, dynamo_decimal_parse, parse_insert_data_rds
from utils.exceptions.controller_exceptions import ControllerException

FIELDS_TABLE = os.environ["FIELDS_TABLE"]
DYNAMO_RESOURCE = boto3.resource("dynamodb", region_name=REGION_NAME)
DYNAMO_CLIENT = boto3.client("dynamodb", region_name=REGION_NAME)
FIELDS = DYNAMO_RESOURCE.Table(FIELDS_TABLE)


class FieldModel:
    def __init__(self, user_id: str, tenant_id: str):
        self.table = "custom_fields_master"
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.fields = {
            "custom_field_id": None,
            "column_name": None,
            "name": None,
            "parent_object_id": None,
            "input": None,
            "data_type": None,
            "format": None,
            "is_required": True,
            "description": None,
            "width": None,
            "height": None,
            "acceptable_values": None,
            "data_source": None,
            "properties": None,
            "is_active": True,
            "created_by": None,
            "created_at": None,
            "updated_by": None,
            "updated_at": None,
        }

    def set_fields(self, payload: dict = None):
        valid_fields: list = self.fields.keys()
        for key in payload.keys():
            if key not in valid_fields:
                return False, "Invalid Request Body"
            else:
                self.fields[key] = payload[key]

    def format_field_column_name(self):
        if not self.fields["column_name"].startswith("cf_"):
            self.fields["column_name"] = f"cf_{self.fields['column_name']}"

    def check_if_field_exist(self):
        sql = (
            SQLTable()
            .select("custom_fields_master", ["custom_field_id"])
            .where(
                [
                    f"column_name = '{self.fields['column_name']}'",
                    f"parent_object_id = '{self.fields['parent_object_id']}'",
                ]
            )
            .query
        )
        result = deserialize_rds_response(rds_execute_statement(sql))
        if len(result) > 0:
            raise ControllerException(
                400,
                {
                    "result": "There is a field with the same configuration",
                    "code": "Field.AlreadyExists",
                },
            )

    def parent_object_exist(self):
        parent_object_name_sql = (
            SQLTable()
            .select("entities_master", ["database_name"])
            .where([f"entity_id = '{self.fields['parent_object_id']}'"])
            .query
        )
        response = deserialize_rds_response(rds_execute_statement(parent_object_name_sql))
        if len(response) == 0:
            raise ControllerException(
                400,
                {"result": "Parent object doesn't exist", "code": "Field.ParentObjectDoesNotExist"},
            )

        parent_object_name = response[0]["database_name"]
        return parent_object_name

    def create_field_record(self):
        self.fields["created_by"] = self.user_id
        self.fields["tenant_id"] = self.tenant_id
        keys = [
            "column_name",
            "name",
            "is_required",
            "parent_object_id",
            "input",
            "data_type",
            "format",
            "description",
            "width",
            "height",
            "data_source",
            "created_by",
            "tenant_id",
        ]
        entity_insert_data = {x: self.fields[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(entity_insert_data)
        sql = SQLTable()
        table = self.table
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("custom_field_id")
            .query
        )
        custom_field_id = execute(query)[0]["custom_field_id"]
        self.fields["custom_field_id"] = custom_field_id
        dynamo_insert = {
            "custom_field_id": self.fields["custom_field_id"],
            "tenant_id": self.tenant_id,
            "parent_object_id": self.fields["parent_object_id"],
        }
        dynamo_field_properties = {**self.fields["properties"]}
        items = dynamo_decimal_parse(dynamo_insert)
        properties = dynamo_decimal_parse(dynamo_field_properties)
        items.update({"properties": properties})
        FIELDS.put_item(Item=items)

    def add_field_to_object(self, parent_object_name):
        sql = SQLTable(
            f"ALTER TABLE {parent_object_name} \
                ADD COLUMN {self.fields['column_name']} {self.fields['data_type']}"
        ).query
        rds_execute_statement(sql)

    def get_parent_object_id(self):
        sql = (
            SQLTable()
            .select("entities_master", ["entity_id"])
            .where([f"database_name = '{self.fields['parent_object']}'"])
            .query
        )
        result = deserialize_rds_response(rds_execute_statement(sql))
        if len(result) == 0:
            raise ControllerException(
                400,
                {"result": "Parent object doesn't exist", "code": "Field.ParentObjectDoesNotExist"},
            )
        return result[0]["entity_id"]

    def get_field_type(self):
        sql = SQLTable()
        table = "field_types"
        where_query = [f"field_type = '{self.fields['input']}'"]

        query = sql.select(table).where(where_query).query
        return execute(query)[0]["data_type"]

    @staticmethod
    def get_field_properties(custom_field_id: str, tenant_id: str) -> dict:
        dynamo_field = FIELDS.get_item(
            Key={"custom_field_id": custom_field_id, "tenant_id": tenant_id}
        )
        if "Item" in dynamo_field:
            properties = json.loads(json.dumps(dynamo_field["Item"], cls=DecimalEncoder))
            return properties
        else:
            raise ControllerException(
                400,
                {
                    "result": "Field doesn't exist",
                    "code": "Field.FieldDoesNotExist",
                },
            )
