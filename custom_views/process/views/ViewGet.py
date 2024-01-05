from controllers.views.schemas.Views import ViewSerializer
from models.Entity import EntityModel
from models.Field import FieldModel

from core_utils.functions import (  # noqa: E501
    UUID,
    deserialize_rds_response,
    rds_execute_statement,
)
from sql_handler.sql_tables_handler import SQLTable
from utils.common import format_audit
from utils.exceptions.controller_exceptions import ControllerException


class ViewGet:
    def __init__(self, view_object: ViewSerializer):  # noqa: E501
        self.view_object = view_object

    def _add_field_data(self, json_content: dict):
        """add each field data if field exists in the json content

        Args:
            json content (dict): the view json content that the frontend processes to show it # noqa: E501

        """
        cards = json_content.get("cards", [])
        new_fields = []

        for card in cards:
            sections = card.get("sections", [])
            for section in sections:
                fields = section.get("fields", [])
                if not fields:
                    continue
                for field in fields:
                    if "custom_field_id" in field:
                        field_model = FieldModel(
                            custom_field_id=field["custom_field_id"],
                            tenant_id=self.view_object.tenant_id,
                        )
                        field_data = field_model.get_field()
                        if ("property_entity_id" in field_data.get("data_source", {})) and (
                            "property_field_id" in field_data.get("data_source", {})
                        ):
                            property_entity_id = field_data["data_source"]["property_entity_id"]
                            property_field_id = field_data["data_source"]["property_field_id"]

                            entity_model = EntityModel(property_entity_id)
                            entity = entity_model.get_entity_by_id()

                            field_column_name = field_model.get_field_column_name(property_field_id)

                            field_data["data_source"] = {
                                "name": entity["database_name"],
                                "api_name": entity["api_name"],
                                "entity_table_primary_key": entity["database_entity_primary_key"],
                                "column_name": field_column_name,
                            }

                        acceptable_values = self._process_field(field_data)
                        field_data.update({"acceptable_values": acceptable_values})
                        new_fields.append(field_data)
                section["fields"] = new_fields.copy()
                new_fields.clear()

    def _add_entity_data(self, entity_id: str) -> dict:
        """Get the entity data for response

        Args:
            entity_id (str): UUID of the view entity

        Returns:
            dict: entity dict
        """
        if entity_id is None:
            raise ControllerException(
                400, {"result": "Entity id can't be null", "code": "EntityIdNull"}  # noqa: E501
            )

        sql = (
            SQLTable()
            .select("entities_master", ["entity_id", "name", "database_name"])
            .where([f"entity_id='{entity_id}'"])
            .query
        )
        entity_data = deserialize_rds_response(rds_execute_statement(sql))[0]
        select_query = ["c.column_name AS database_entity_primary_key"]
        table = "information_schema.key_column_usage AS c"
        left_join_query = "information_schema.table_constraints AS \
            t ON t.constraint_name = c.constraint_name"
        where_query = [
            f"t.table_name = '{entity_data.get('database_name')}'",
            "t.constraint_type = 'PRIMARY KEY'",
        ]
        query = (
            SQLTable()
            .select(table, select_query)
            .left_join(left_join_query)
            .where(where_query)
            .query
        )
        primary_key = deserialize_rds_response(rds_execute_statement(query))[0]
        entity_data.update(primary_key)
        # Return empty dict if configuration not found
        return entity_data

    def _process_field(self, field: dict) -> list:
        properties = field.get("properties", {})
        is_picklist = properties.get("is_picklist", False)

        if is_picklist:
            return FieldModel(
                custom_field_id=field.get("custom_field_id"),
                tenant_id=self.view_object.tenant_id,
            ).get_picklist_values()
        else:
            return []

    def perform(self) -> tuple:
        view = self.view_object.get_view()
        format_audit([view])
        if not len(view):
            raise ControllerException(
                404, {"result": "View not found", "code": "ViewDoesNotExist"}
            )  # noqa: E501
        # Add audit data and json content from Dynamo
        view["entity"] = self._add_entity_data(view.pop("entity_id", None))
        self._add_field_data(view["json_content"])

        return (
            200,
            {"correlation_id": UUID, "result": "Get view success", "view": view},  # noqa: E501
        )
