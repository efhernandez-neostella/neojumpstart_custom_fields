from aws_lambda_powertools.utilities.parser import validator
from controllers.views.schemas.Views import ViewSerializer
from views_utils import is_uuid_valid

from core_utils.functions import UUID
from utils.exceptions.controller_exceptions import ControllerException
from utils.models import RequestEventModel, ResponseModel


class ViewGetModel(ViewSerializer):
    view_id: str
    
    @validator("view_id")
    def validate_view_id(cls, value):
        if not is_uuid_valid(value):
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "error": "view_id is not a valid UUID",
                    "code": "ViewGet.InvalidViewId"
                }
            )
        return value


class ViewGetRequestEventModel(RequestEventModel):
    pathParameters: ViewGetModel

    @validator("pathParameters", pre=True, always=True)
    def set_pathParameters(cls, pathParameters):
        return pathParameters or {}


class ViewGetResponse(ResponseModel):
    view: ViewSerializer
