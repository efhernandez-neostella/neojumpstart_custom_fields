import json
import os
import re

import boto3
from fields_utils import dynamo_decimal_parse

from core_utils.functions import (
    REGION_NAME,
    deserialize_rds_response,
    rds_execute_statement,
)
from sql_handler.sql_tables_handler import SQLTable, execute
from sql_handler.sql_views_handler import SQLView
from utils.common import (
    DecimalEncoder,
    dynamo_execute_statement,
    format_audit,
    get_user_timezone,
    parse_insert_data_rds,
    parse_update_data_rds,
)
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
            "column_name": None,  # cf_...
            "name": None,
            "parent_object_id": None,
            "input": None,
            "data_type": None,
            "format": None,
            "is_required": True,
            "description": None,
            "width": None,
            "height": None,
            "is_select": False,
            "property_schemas_id": None,
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

    def validate_post_payload(self, payload):
        self.set_fields(payload=payload)

        if (
            self.fields["column_name"] is None
            or len(self.fields["column_name"]) == 0
            or not isinstance(self.fields["column_name"], str)
        ):
            return False, "Field.ColumnNameIsRequired"
        elif re.match("^[a-z0-9]$", self.fields["column_name"]):
            return False, "Field.ColumnNameInvalidFormat"

        if (
            self.fields["name"] is None
            or len(self.fields["name"]) == 0
            or not isinstance(self.fields["name"], str)
        ):
            return False, "Field.NameIsRequired"

        if (
            self.fields["parent_object_id"] is None
            or len(self.fields["parent_object_id"]) == 0
            or not isinstance(self.fields["parent_object_id"], str)
        ):
            return False, "Field.ParentObjectIdIsRequired"

        if (
            self.fields["input"] is None
            or len(self.fields["input"]) == 0
            or not isinstance(self.fields["input"], str)
        ):
            return False, "Field.InputIsRequired"

        if (
            self.fields["data_type"] is None
            or len(self.fields["data_type"]) == 0
            or not isinstance(self.fields["data_type"], str)
        ):
            return False, "Field.DataTypeIsRequired"

        if self.fields["format"] is not None:
            if len(self.fields["format"]) == 0 or not isinstance(self.fields["format"], str):
                return False, "Field.FormatEmptyValue"

        if self.fields["description"] is not None:
            if not isinstance(self.fields["description"], str):
                return False, "Field.DescriptionBadValue"

        if self.fields["width"] is not None:
            if not isinstance(self.fields["width"], str):
                return False, "Field.WidthBadValue"

        if self.fields["height"] is not None:
            if not isinstance(self.fields["height"], str):
                return False, "Field.HeightBadValue"

        if not isinstance(self.fields["properties"], dict):
            return False, "Field.PropertiesBadValue"

        if self.fields["is_required"] is None or not isinstance(self.fields["is_required"], bool):
            return False, "Field.IsRequiredBadValue"

        return True, ""

    def validate_id_payload(self, payload):
        if (
            "custom_field_id" not in payload
            or payload["custom_field_id"] is None
            or not isinstance(payload["custom_field_id"], str)
            or len(payload["custom_field_id"]) == 0
        ):
            raise ControllerException(
                400,
                {"result": "Custom field id is required", "code": "Field.CustomFieldIdRequired"},
            )
        self.fields["custom_field_id"] = payload["custom_field_id"]

    def fields_get_validate_payload(self, payload: dict = None):
        valid_fields: list = self.fields.keys()

        # Custom filters
        self.fields["parent_object"] = None

        for key in payload.keys():
            if key not in valid_fields:
                return False, "Invalid Request Body"
            else:
                self.fields[key] = payload[key]  # Populate self.fields object

        if isinstance(self.fields["is_active"], str):
            if self.fields["is_active"] not in ["true", "false"]:
                return False, "Field.IsActiveInvalidValue"

        if self.fields["parent_object"] is not None:
            if not isinstance(self.fields["parent_object"], str):
                return False, "Field.ParentObjectInvalidValue"

        return True, ""

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
        field_insert_data = {x: self.fields[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(field_insert_data)
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
            # Used for filter
            "parent_object_id": self.fields["parent_object_id"],
        }
        dynamo_field_properties = {**self.fields["properties"]}
        items = dynamo_decimal_parse(dynamo_insert)
        properties = dynamo_decimal_parse(dynamo_field_properties)
        items.update({"properties": properties})
        FIELDS.put_item(Item=items)

    def update_field_record(self, keys: list):
        self.fields["updated_by"] = self.user_id
        self.fields["updated_at"] = "NOW()"
        field_update_data = {x: self.fields[x] for x in keys}
        parsed_update_data = parse_update_data_rds(field_update_data)
        sql = SQLTable()
        table = self.table
        where_query = [f"custom_field_id = '{self.fields['custom_field_id']}'"]
        query = (
            sql.update(table, parsed_update_data)
            .where(where_query)
            .return_value("custom_field_id")
            .query
        )
        dynamo_update = {
            "custom_field_id": self.fields["custom_field_id"],
            "tenant_id": self.tenant_id,
            "parent_object_id": self.fields["parent_object_id"],
        }
        dynamo_field_properties = {**self.fields["properties"]}
        items = dynamo_decimal_parse(dynamo_update)
        properties = dynamo_decimal_parse(dynamo_field_properties)
        items.update({"properties": properties})
        FIELDS.update_item(
            Key={"custom_field_id": self.fields["custom_field_id"], "tenant_id": self.tenant_id},
            UpdateExpression="SET parent_object_id = :parent_object_id, properties = :properties",
            ExpressionAttributeValues={
                ":parent_object_id": self.fields["parent_object_id"],
                ":properties": self.fields["properties"],
            },
            ReturnValues="ALL_NEW",
        )
        return execute(query)[0]

    def add_field_to_object(self, parent_object_name):
        sql = SQLTable(
            f"ALTER TABLE {parent_object_name} \
                ADD COLUMN {self.fields['column_name']} {self.fields['data_type']}"
        ).query
        rds_execute_statement(sql)

    def add_foreign_field_to_object(self, parent_object_name: str, entity_data: dict):
        column_name = self.fields["column_name"]
        property_entity_database_name = entity_data.get("database_name")
        property_entity_primary_key = entity_data.get("database_entity_primary_key")
        sql = SQLTable(
            f"""ALTER TABLE {parent_object_name}
                    ADD COLUMN {column_name} uuid NULL,
                    ADD CONSTRAINT fk_{column_name} FOREIGN KEY({column_name})
                    REFERENCES {property_entity_database_name}({property_entity_primary_key});"""
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

    def get_fields(self):
        sql_condition = f"c.tenant_id = '{self.tenant_id}'"
        parent_condition = ""
        if isinstance(self.fields["is_active"], str):
            sql_condition += f" AND c.is_active='{self.fields['is_active']}'"
        if isinstance(self.fields["parent_object"], str):
            parent_object_id = self.get_parent_object_id()
            parent_condition = f" AND \"parent_object_id\" = '{parent_object_id}'"
            sql_condition += f" AND c.parent_object_id='{parent_object_id}'"

        # Hotfix. This needs to be improved
        elif self.fields["parent_object_id"] and isinstance(self.fields["parent_object_id"], str):
            parent_condition = (
                f""" AND \"parent_object_id\" = '{self.fields["parent_object_id"]}'"""
            )
            sql_condition += f""" AND c.parent_object_id='{self.fields["parent_object_id"]}'"""

        dynamo_sql = f'SELECT * FROM "{FIELDS_TABLE}" WHERE "tenant_id" = \'{self.tenant_id}\' {parent_condition}'
        dynamo_fields = dynamo_execute_statement(dynamo_sql)
        dynamo_fields = json.loads(json.dumps(dynamo_fields, cls=DecimalEncoder))
        time_zone = get_user_timezone(user_id=self.user_id)

        sql = (
            SQLView()
            .select(
                f"""
            c.custom_field_id, c.is_active, c.column_name, c.name,
            c.parent_object_id, o.name as parent_object_name, c.input, c.data_type, c.format,
            c.is_required, c.description, c.width, c.height, c.data_source,
            c.tenant_id, c.created_by, c.updated_by,
            u.full_name as created_by_user_name,
            to_char(c.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
            'MM/DD/YYYY HH24:MI:SS.MS') AS created_at,
            u2.full_name as updated_by_user_name,
            to_char(c.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
            'MM/DD/YYYY HH24:MI:SS.MS') AS updated_at
            FROM {self.table} c
        """
            )
            .left_join("users_master u ON c.created_by = u.cognito_user_id")
            .left_join("entities_master o ON o.entity_id = c.parent_object_id")
            .left_join("users_master u2 ON c.updated_by = u2.cognito_user_id")
            .where(sql_condition)
            .query
        )
        rds_fields = deserialize_rds_response(rds_execute_statement(sql))
        format_audit(rds_fields)
        for field in rds_fields:
            field_intersection = list(
                filter(
                    lambda _field: _field["custom_field_id"] == field["custom_field_id"],
                    dynamo_fields,
                )
            )[0]
            field["properties"] = field_intersection.get("properties", {})

        return rds_fields

    def get_field(self):
        dynamo_field = FIELDS.get_item(
            Key={"custom_field_id": self.fields["custom_field_id"], "tenant_id": self.tenant_id}
        )
        if "Item" in dynamo_field:
            dynamo_field = json.loads(json.dumps(dynamo_field["Item"], cls=DecimalEncoder))
            time_zone = get_user_timezone(user_id=self.user_id)
            sql = (
                SQLView()
                .select(
                    f"""
                c.custom_field_id, c.is_active, c.column_name, c.name,
                c.parent_object_id, o.name as parent_object_name, c.input, c.data_type, c.format,
                c.is_required, c.description, c.width, c.height, c.data_source,
                c.tenant_id, c.created_by, c.updated_by,
                u.full_name as created_by_user_name,
                to_char(c.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
                'MM/DD/YYYY HH24:MI:SS.MS') AS created_at,
                u2.full_name as updated_by_user_name,
                to_char(c.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
                'MM/DD/YYYY HH24:MI:SS.MS') AS updated_at
                FROM {self.table} c
            """
                )
                .left_join("users_master u ON c.created_by = u.cognito_user_id")
                .left_join("entities_master o ON o.entity_id = c.parent_object_id")
                .left_join("users_master u2 ON c.updated_by = u2.cognito_user_id")
                .where(f"c.custom_field_id = '{self.fields['custom_field_id']}'")
                .query
            )
            rds_field = deserialize_rds_response(rds_execute_statement(sql))
            format_audit(rds_field)
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

    def create_property_schema(self, insert_data: dict):
        insert_data["created_by"] = self.user_id
        keys = [
            "property_entity_id",
            "property_field_id",
            "is_multiple",
            "is_picklist",
            "created_by",
        ]
        property_schema_insert_data = {x: insert_data[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(property_schema_insert_data)
        sql = SQLTable()
        table = "property_schemas"
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("property_schemas_id")
            .query
        )
        property_schemas_id = execute(query)[0]["property_schemas_id"]
        return property_schemas_id

    def update_property_schema(self, update_data: dict):
        update_data["updated_by"] = self.user_id
        update_data["updated_at"] = "NOW()"
        keys = ["linking_tables_id", "updated_by", "updated_at"]
        property_schema_update_data = {x: update_data[x] for x in keys}
        parsed_update_data = parse_update_data_rds(property_schema_update_data)
        sql = SQLTable()
        table = "property_schemas"
        where_query = [f"property_schemas_id = '{self.fields['property_schemas_id']}'"]
        query = (
            sql.update(table, parsed_update_data)
            .where(where_query)
            .return_value("property_schemas_id")
            .query
        )
        return execute(query)[0]

    def get_entity_linking_table_data(self, entity_id: str):
        select_query = ["em.database_name", "c.column_name AS database_entity_primary_key"]
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

    def get_linking_table(self, table_name_1: str, table_name_2: str):
        select_query = ["linking_tables_id"]
        table = "linking_tables"
        where_query = [
            f"linking_table_name = '{table_name_1}' OR linking_table_name = '{table_name_2}'"
        ]
        query = SQLTable().select(table, select_query).where(where_query).query
        return execute(query, table, False)

    def insert_linking_table(self, insert_data):
        keys = ["linking_table_name"]
        linking_table_insert_data = {x: insert_data[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(linking_table_insert_data)
        sql = SQLTable()
        table = "linking_tables"
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("linking_tables_id")
            .query
        )
        return execute(query)[0]

    def create_linking_table(self, data: dict):
        entity_1 = data["entity_1"].get("database_name")
        entity_id_1 = data["entity_1"].get("database_entity_primary_key")
        entity_2 = data["entity_2"].get("database_name")
        entity_id_2 = data["entity_2"].get("database_entity_primary_key")
        query = f"""CREATE TABLE IF NOT EXISTS {entity_1}_{entity_2} (
        {entity_1}_{entity_2}_id uuid DEFAULT uuid_generate_v4 () PRIMARY KEY,
        {entity_id_1} uuid NOT NULL,
        {entity_id_2} uuid NOT NULL,
        custom_field_id uuid NOT NULL,
        CONSTRAINT fk_{entity_id_1} FOREIGN KEY({entity_id_1}) REFERENCES {entity_1}({entity_id_1}),
        CONSTRAINT fk_{entity_id_2} FOREIGN KEY({entity_id_2}) REFERENCES {entity_2}({entity_id_2}),
        CONSTRAINT fk_custom_field_id FOREIGN KEY(custom_field_id) 
        REFERENCES custom_fields_master(custom_field_id)
        );"""
        rds_execute_statement(query)
        return {"linking_table_name": f"{entity_1}_{entity_2}"}

    def get_custom_field(self, field_id: str):
        table = "custom_fields_master"
        where_query = [f"custom_field_id = '{field_id}'"]
        query = SQLTable().select(table).where(where_query).query
        return execute(query, table, False)

    def get_picklist_values(self, field_id: str):
        table = "picklist_master"
        select_query = ["picklist_id", "name", "is_active"]
        where_query = [f"custom_field_id = '{field_id}'"]
        query = (
            SQLTable().select(table, select_query).where(where_query).order_by("order_num").query
        )
        return execute(query, table, False)

    def insert_picklist_values(self, insert_data: dict) -> dict:
        insert_data["custom_field_id"] = self.fields["custom_field_id"]
        keys = ["name", "is_active", "custom_field_id", "order_num"]
        picklist_table_insert_data = {x: insert_data[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(picklist_table_insert_data)
        sql = SQLTable()
        table = "picklist_master"
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("picklist_id")
            .query
        )
        return execute(query)[0]

    def update_picklist_values(self, picklist_id: str, update_data: dict) -> dict:
        update_data["custom_field_id"] = self.fields["custom_field_id"]
        keys = ["name", "is_active", "custom_field_id", "order_num"]
        picklist_table_update_data = {x: update_data[x] for x in keys}
        parsed_update_data = parse_update_data_rds(picklist_table_update_data)
        sql = SQLTable()
        table = "picklist_master"
        where_query = [f"picklist_id = '{picklist_id}'"]
        query = (
            sql.update(table, parsed_update_data)
            .where(where_query)
            .return_value("picklist_id")
            .query
        )
        return execute(query)[0]
