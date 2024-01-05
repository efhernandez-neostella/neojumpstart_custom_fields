from aws_lambda_powertools.utilities.parser import validator
from pydantic import Json

from utils.models import ResponseModel, RequestEventModel
from controllers.views.schemas.Views import ViewSerializer


class ViewPutModel(ViewSerializer):
    view_id: str


class ViewPutRequestEventModel(RequestEventModel):
    body: Json[ViewPutModel]

    @validator("body", pre=True, always=True)
    def set_body(cls, body):
        return body or "{}"


class ViewPutResponse(ResponseModel):
    view_id: str
