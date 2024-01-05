from sql_handler.sql_tables_handler import SQLTable, execute
from utils.common import (format_audit, format_nulleable_values,
                          get_user_timezone, parse_insert_data_rds)
from utils.exceptions.controller_exceptions import ControllerException


class FieldTypeModel:
    def __init__(self, user_id: str, tenant_id: str):
        self.table = "field_types"
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.fields = {
            "field_type_id": None,
            "field_type": None,
            "data_type": None,
        }

    def perform_get_list(self) -> list:
        """Gets a list of all available field types for custom_fields"""

        field_types_list = self.get_field_types()
        return field_types_list
    
    def get_field_types(self):
        sql = SQLTable()
        table = self.table
        query = (
            sql.select(table).query
        )
        return execute(query, table, False)