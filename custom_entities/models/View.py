from sql_handler.sql_tables_handler import SQLTable, execute
from utils.common import parse_insert_data_rds


class ViewModel:
    def __init__(self, user_id: str, tenant_id: str, entity_id: str):
        self.table = "views_master"
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.entity_id = entity_id
        self.fields = {
            "view_id":  None,
            "view_name": None,
            "entity_id": entity_id,
            "view_endpoint": None,
            "view_route": None,
            "is_active": True,
            "tenant_id": tenant_id,
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

    def create_view(self):
        self.fields["created_by"] = self.user_id
        keys = [
            "view_name",
            "entity_id",
            "view_endpoint",
            "view_route",
            "tenant_id",
            "created_by"
        ]
        view_insert_data = {x: self.fields[x] for x in keys}
        parsed_insert_data = parse_insert_data_rds(view_insert_data)
        sql = SQLTable()
        query = (
            sql.insert(self.table, list(parsed_insert_data.keys()))
            .values(list(parsed_insert_data.values()))
            .return_value("view_id")
            .query
        )
        view_id = execute(query)[0]["view_id"]
        return view_id
