from entities_utils import to_snake_format
from models.View import ViewModel


class ViewsPost:
    def __init__(self, view_object: "ViewModel", view_type: str, payload: dict):
        self.view_object = view_object
        self.view_type = view_type
        self.payload = payload

    def perform(self):
        if self.view_type == "list":
            view_route = f"/{self.payload['entity_name']}"
            view_endpoint = f"/custom{view_route}"
            return self._insert_view(view_route, view_endpoint)
        elif self.view_type == "create":
            view_route = f"/{self.payload['entity_name']}/create"
            view_endpoint = f"/custom/{self.payload['entity_name']}"
            return self._insert_view(view_route, view_endpoint)
        elif self.view_type == "update":
            view_route = f"/{self.payload['entity_name']}" + "/{id}"
            view_endpoint = f"/custom{view_route}"
            return self._insert_view(view_route, view_endpoint)
        else:
            return False, "Invalid view type"

    def _insert_view(self, view_route, view_endpoint):
        view_name = f"{to_snake_format(self.payload['entity_name'])}_{self.view_type}_view"
        self.payload.update(
            {"view_name": view_name, "view_route": view_route, "view_endpoint": view_endpoint}
        )
        insert_data = self._setup_insert_data()
        self.view_object.set_fields(insert_data)
        view_id = self.view_object.create_view()
        return view_id, "View successfully created"

    def _setup_insert_data(self):
        view_data = {**self.payload}
        view_data.pop("entity_name")
        return view_data
