from aws_lambda_powertools.utilities.parser import validator
from pydantic import Json

from utils.models import ResponseModel, RequestEventModel
from controllers.views.schemas.Views import ViewSerializer


class ViewPostModel(ViewSerializer):
    view_id: str
    view_name: str
    entity_id: str


class ViewPostRequestEventModel(RequestEventModel):
    body: Json[ViewPostModel]

    @validator("body", pre=True, always=True)
    def set_body(cls, body):
        return body or "{}"


class ViewPostResponse(ResponseModel):
    view_id: str
