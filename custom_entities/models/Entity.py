from typing import Dict, List

from core_utils.functions import deserialize_rds_response, rds_execute_statement
from sql_handler.sql_tables_handler import SQLTable, execute
from utils.common import (
    format_audit,
    format_for_rds,
    get_user_timezone,
    parse_insert_data_rds,
    parse_update_data_rds,
)
from utils.exceptions.controller_exceptions import ControllerException


class EntityModel:
    def __init__(self, user_id: str, tenant_id: str):
        self.table = "entities_master"
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.fields = {
            "entity_id": None,
            "name": None,
            "database_name": None,
            "translation_reference": None,
            "api_name": None,
            "can_edit_fields": True,
            "can_edit_views": True,
            "can_remove": True,
            "can_soft_delete": True,
            "is_child": False,
            "has_file_storage": True,
            "parent_entity_id": None,
            "component_id": None,
            "linking_table": False,
            "entity_limit": 0,
            "is_base": False,
            "is_active": True,
            "icon_name": None,
            "icon_class": None,
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

    def entities_get_validate_payload(self, payload: dict = None):
        self.set_fields(payload=payload)

        if isinstance(self.fields["is_active"], str):
            if self.fields["is_active"] not in ["true", "false"]:
                return False, "Entity.IsActiveInvalidValue"

        return True, ""

    def entity_get_validate_payload(self, payload: dict = None):
        self.set_fields(payload=payload)

        if (
            self.fields["entity_id"] is None
            or len(self.fields["entity_id"]) == 0
            or not isinstance(self.fields["entity_id"], str)
        ):
            return False, "Entity.EntityIdIsRequired"

        return True, ""

    def entities_views_select_query(self) -> SQLTable:
        """Function that returns the entity object initialized

        Returns:
            SQLTable: SQL object with the entity data
        """
        return SQLTable().select(
            f"{self.table} e",
            ["e.entity_id", "e.name", "e.in_drawer", "e.icon_name", "e.api_name", "database_name"],
        )

    def get_routes(self, entities: List[Dict]) -> None:
        """
        Function that updates the entities object with its related views and view routes

        Args:
            entities (List[Dict]): list of entity objects to add the views related
        """
        for entity in entities:
            sql = (
                SQLTable()
                .select(
                    "views_master v",
                    [
                        "v.view_id",
                        "v.view_name",
                        "v.view_route",
                    ],
                )
                .where([f"v.entity_id={format_for_rds(entity['entity_id'])}"])
                .query
            )
            entity["views"] = deserialize_rds_response(rds_execute_statement(sql))

    def get_endpoints(self) -> str:
        select_query = [
            "json_agg(json_build_object('view_id', v.view_id,\
                        'view_name', v.view_name, 'view_route', v.view_route,\
                        'view_endpoint', v.view_endpoint))"
        ]
        sql = (
            SQLTable()
            .select(
                "views_master v",
                select_query,
            )
            .where([f"v.entity_id={format_for_rds(self.fields['entity_id'])}"])
            .query
        )
        return sql

    def get_fields(self) -> str:
        select_query = [
            "json_agg(json_build_object(\
                        'custom_field_id', f.custom_field_id,\
                        'column_name', f.column_name, 'name', f.name,\
                        'input', f.input, 'description', f.description,\
                        'is_required', f.is_required))"
        ]
        sql = (
            SQLTable()
            .select(
                "custom_fields_master f",
                select_query,
            )
            .where([f"f.parent_object_id={format_for_rds(self.fields['entity_id'])}"])
            .query
        )
        return sql

    def get_entities_views(self) -> List[Dict]:
        """Function to get the entities and add its views

        Returns:
            List[Dict]: The entity list data
        """
        sql_object = self.entities_views_select_query()
        entities = deserialize_rds_response(rds_execute_statement(sql_object.query))

        # Add all entity views
        self.get_routes(entities)

        return entities

    def get_entities(self):
        sql_condition = ""
        if isinstance(self.fields["is_active"], str):
            sql_condition += f" e.is_active = {self.fields['is_active']} "

        time_zone = get_user_timezone(user_id=self.user_id)

        sql = SQLTable()
        table = f"{self.table} e"
        select_query = [
            "e.*",
            "u.full_name as created_by_user_name",
            "u2.full_name as updated_by_user_name",
            f"""to_char(e.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
                'MM/DD/YYYY HH24:MI:SS.MS') AS created_at""",
            f"""to_char(e.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
                'MM/DD/YYYY HH24:MI:SS.MS') AS updated_at""",
        ]

        left_join_query = "users_master u\
            ON e.created_by = u.cognito_user_id"
        left_join_query_2 = "users_master u2\
            ON e.updated_by = u2.cognito_user_id"

        query = (
            sql.select(table, select_query)
            .left_join(left_join_query)
            .left_join(left_join_query_2)
        )

        if len(sql_condition) > 0:
            query = query.where(sql_condition)
        order_by_query = "e.created_at"
        query = query.order_by(order_by_query)
        rds_entities = deserialize_rds_response(rds_execute_statement(query.query))
        format_audit(rds_entities)
        return rds_entities

    def get_entity(self):
        time_zone = get_user_timezone(user_id=self.user_id)

        sql = SQLTable()
        table = f"{self.table} e"
        select_query = [
            "e.*",
            "u.full_name AS created_by_user_name",
            "u2.full_name AS updated_by_user_name",
            f"""to_char(e.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
                'MM/DD/YYYY HH24:MI:SS.MS') AS created_at""",
            f"""to_char(e.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}',
                'MM/DD/YYYY HH24:MI:SS.MS') AS updated_at""",
            f"({self.get_endpoints()}) AS views",
            f"({self.get_fields()}) AS fields",
            f"({self.get_columns()}) AS columns",
        ]
        left_join_query = "users_master u ON e.created_by = u.cognito_user_id"
        left_join_query_2 = "users_master u2 ON e.updated_by = u2.cognito_user_id"
        where_query = [f"e.entity_id = '{self.fields['entity_id']}'"]

        query = (
            sql.select(table, select_query)
            .left_join(left_join_query)
            .left_join(left_join_query_2)
            .where(where_query)
        )
        rds_entity = deserialize_rds_response(rds_execute_statement(query.query))

        if len(rds_entity) == 0:
            raise ControllerException(
                400,
                {
                    "result": f"Entity with entity_id {self.fields['entity_id']} doesn't exist"
                },
            )

        primary_key = self.get_table_primary_key(rds_entity[0].get("database_name"))
        rds_entity[0].update(primary_key)
        format_audit(rds_entity)

        return rds_entity[0]

    def check_if_entity_exists(self):
        sql = SQLTable()
        table = self.table
        select_query = ["COUNT(*) as result"]
        where_query = [
            f"api_name = '{self.fields['api_name']}' \
                OR name = '{self.fields['name']}' \
                    OR database_name = '{self.fields['database_name']}'"
        ]
        query = sql.select(table, select_query).where(where_query).query
        result = execute(query)[0]["result"]
        if result > 0:
            raise ControllerException(
                400, {"result": "There is a entity with the same configuration"}
            )

    def create_entity(self):
        self.fields["created_by"] = self.user_id
        keys = [
            "name",
            "database_name",
            "translation_reference",
            "api_name",
            "can_edit_fields",
            "can_edit_views",
            "can_remove",
            "can_soft_delete",
            "has_file_storage",
            "component_id",
            "linking_table",
            "is_base",
            "is_active",
            "entity_limit",
            "icon_name",
            "icon_class",
            "created_by",
        ]
        entity_insert_data = {x: self.fields[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(entity_insert_data)
        sql = SQLTable()
        table = self.table
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("entity_id")
            .query
        )
        entity_id = execute(query)[0]["entity_id"]

        sql_table = f"""CREATE TABLE {self.fields['database_name']} (
            {self.fields['database_name']}_id UUID DEFAULT uuid_generate_v4 () PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW(),
            created_by uuid NOT NULL,
            updated_at TIMESTAMP DEFAULT NULL,
            updated_by uuid DEFAULT NULL,
		    CONSTRAINT fk_created_by FOREIGN KEY(created_by)
            REFERENCES users_master(cognito_user_id),
            CONSTRAINT fk_updated_by FOREIGN KEY(updated_by)
            REFERENCES users_master(cognito_user_id)
        );"""
        rds_execute_statement(sql_table)

        return entity_id

    def update_entity(self):
        self.fields["updated_by"] = self.user_id
        self.fields["updated_at"] = "NOW()"
        keys = [
            "name",
            "can_edit_fields",
            "can_edit_views",
            "can_remove",
            "can_soft_delete",
            "has_file_storage",
            "component_id",
            "linking_table",
            "is_base",
            "is_active",
            "entity_limit",
            "icon_name",
            "icon_class",
            "updated_by",
            "updated_at",
        ]
        entity_update_data = {x: self.fields[x] for x in keys}
        parsed_update_data = parse_update_data_rds(entity_update_data)
        sql = SQLTable()
        table = self.table
        where_query = [f"entity_id = '{self.fields['entity_id']}'"]
        query = (
            sql.update(table, parsed_update_data)
            .where(where_query)
            .return_value("entity_id")
            .query
        )
        return execute(query)[0]

    def get_entity_conditional(
        self, select_query: list = [], conditional: list = []
    ) -> list:
        sql = SQLTable()
        table = self.table
        query_builder = sql.select(table, select_query)
        if conditional:
            query_builder = query_builder.where(conditional)
        query = query_builder.query
        result = execute(query, table, False)
        return result if len(result) > 0 else {}

    def get_columns(self) -> str:
        sql = SQLTable()
        table = "information_schema.columns"
        select_query = [
            "json_agg(json_build_object('column_name', column_name, \
            'data_type', data_type, 'is_nullable', is_nullable))"
        ]
        where_query = [
            "table_schema = 'public'",
            "table_name = e.database_name",
            "column_name NOT LIKE 'cf_%'",
        ]
        query = sql.select(table, select_query).where(where_query).query
        return query

    def get_table_primary_key(self, database_name: str):
        select_query = ["c.column_name AS database_entity_primary_key"]
        table = "information_schema.key_column_usage AS c"
        left_join_query = "information_schema.table_constraints AS \
            t ON t.constraint_name = c.constraint_name"
        where_query = [
            f"t.table_name = '{database_name}'",
            "t.constraint_type = 'PRIMARY KEY'",
        ]
        query = (
            SQLTable()
            .select(table, select_query)
            .left_join(left_join_query)
            .where(where_query)
            .query
        )
        return deserialize_rds_response(rds_execute_statement(query))[0]

    def create_component_master_permissions(self) -> str:
        component_insert_data = {
            "is_active": True,
            "module": self.fields["database_name"],
            "component": "general",
            "subcomponent": "general",
            "valid_for": "both",
        }
        parsed_insert_data = parse_insert_data_rds(component_insert_data)
        sql = SQLTable()
        table = "components_master"
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("components_id")
            .query
        )
        component_id = execute(query)[0]["components_id"]
        return component_id

    def create_object_master(self, component_id: str) -> None:
        role_insert_data = {
            "table_name": self.fields["database_name"],
            "object_limit": 0,
            "friendly_name_column": self.fields["name"],
            "component_id": component_id,
            "tenant_id": self.tenant_id,
            "created_by": self.user_id,
        }
        parsed_insert_data = parse_insert_data_rds(role_insert_data)
        sql = SQLTable()
        table = "objects_master"
        query = (
            sql.insert(table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("object_id")
            .query
        )
        execute(query, table, False)
