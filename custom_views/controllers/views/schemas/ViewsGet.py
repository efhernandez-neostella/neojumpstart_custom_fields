from typing import List

from aws_lambda_powertools.utilities.parser import validator

from utils.models import ResponseModel, RequestEventModel
from controllers.views.schemas.Views import ViewSerializer


class ViewListRequestEventModel(RequestEventModel):
    queryStringParameters: ViewSerializer

    @validator("queryStringParameters", pre=True, always=True)
    def set_queryStringParameters(cls, queryStringParameters):
        return queryStringParameters or {}


class ViewsListResponse(ResponseModel):
    views: List[ViewSerializer]
