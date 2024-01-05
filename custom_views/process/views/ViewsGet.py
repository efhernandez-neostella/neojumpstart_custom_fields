from controllers.views.schemas.Views import ViewSerializer
from core_utils.functions import UUID
from utils.common import format_audit


class ViewsGet:
    def __init__(self, view_object: ViewSerializer):
        self.view_object = view_object

    def perform(self) -> tuple:
        views = self.view_object.get_views()
        format_audit(views)
        return (
            200,
            {"correlation_id": UUID, "views": views, "result": "Get views success"},
        )
