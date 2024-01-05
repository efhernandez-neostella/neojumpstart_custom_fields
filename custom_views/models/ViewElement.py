from typing import List, Dict, Optional

from aws_lambda_powertools.utilities.parser import BaseModel, Field

from core_utils.functions import deserialize_rds_response, rds_execute_statement
from sql_handler.sql_tables_handler import SQLTable
from utils.common import parse_update_data_rds


class ViewElementModel(BaseModel):
    view_element_id: Optional[str]
    view_element_name: Optional[str]
    key_attributes: Optional[Dict]
    table: str = Field(default="view_elements", exclude=True)

    def _view_elements_query(self) -> SQLTable:
        return SQLTable().select(
            f"{self.table} ve",
            [
                "ve.view_element_id",
                "ve.view_element_name",
                "ve.key_attributes",
            ],
        )

    def get_view_elements(self) -> List[Dict]:
        filters = parse_update_data_rds(self.dict(exclude={"key_attributes"}, exclude_unset=True))

        sql = (self._view_elements_query().where(filters)).query

        views = deserialize_rds_response(rds_execute_statement(sql))
        return views
