import json
from typing import Dict, List, Optional

from aws_lambda_powertools.utilities.parser import BaseModel, Field
from views_utils import VIEWS

from core_utils.functions import (deserialize_rds_response,
                                  rds_execute_statement)
from sql_handler.sql_tables_handler import SQLTable
from utils.common import DecimalEncoder, format_for_rds, parse_update_data_rds
from utils.exceptions.controller_exceptions import ControllerException


class ViewModel(BaseModel):
    view_id: Optional[str]
    view_name: Optional[str]
    entity_id: Optional[str]
    view_endpoint: Optional[str]
    view_route: Optional[str]
    is_active: Optional[bool]
    tenant_id: Optional[str]
    created_by: Optional[str]
    created_at: Optional[str]
    updated_by: Optional[str]
    updated_at: Optional[str]
    table: str = Field(default="views_master", exclude=True)
    time_zone: str = Field(default="UTC", exclude=True)

    def _views_query(self) -> SQLTable:
        return SQLTable().select(
            f"{self.table} v",
            [
                "v.view_id",
                "v.view_name",
                "v.entity_id",
                "v.view_endpoint",
                "v.view_route",
                "v.is_active",
                "v.created_by",
                f"v.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{self.time_zone}' AS created_at",
                "v.updated_by",
                f"v.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{self.time_zone}' AS updated_at",
            ],
        )

    def _get_view_conf(self) -> dict:
        """search the view configuration stored in Dynamo

        Returns:
            dict: configuration dict
        """
        dynamo_view = VIEWS.get_item(
            Key={"view_id": self.view_id, "tenant_id": self.tenant_id}
        )  # noqa: E501
        if "Item" in dynamo_view:
            dynamo_view = json.loads(
                json.dumps(dynamo_view["Item"], cls=DecimalEncoder)
            )  # noqa: E501

            # Return empty dict if configuration not found
            if "json_content" not in dynamo_view:
                return {}
            return dynamo_view["json_content"]
        else:
            # Return empty dict if configuration not found
            return {}

    def get_view(self) -> List[Dict]:
        view = self.get_views()
        if len(view) != 1:
            raise ControllerException(
                400,
                {
                    "result": "View does not exist",
                    "code": "View.DoesNotExist",
                },
            )

        view[0]["json_content"] = self._get_view_conf()
        if not view[0]["json_content"].get("cards"):
            view[0]["json_content"] = {"cards": []}
        return view[0]

    def get_views(self) -> List[Dict]:
        filters = parse_update_data_rds(self.dict(exclude_unset=True))

        sql = (self._views_query().where(filters)).query

        views = deserialize_rds_response(rds_execute_statement(sql))
        return views

    def put_views(self) -> None:
        # Get not null nor unset values
        update_values = self.dict(exclude={"view_id"}, exclude_unset=True)

        # If there's no values to update, don't execute the query as it will raise an error
        if not update_values:
            return

        sql = (
            SQLTable()
            .update(self.table, parse_update_data_rds(update_values))
            .where([f"view_id={format_for_rds(self.view_id)}"])
        ).query
        rds_execute_statement(sql)
