from typing import List, Optional, Dict

from aws_lambda_powertools.utilities.parser import validator, BaseModel

from utils.models import ResponseModel, RequestEventModel


class ViewElementSerializer(BaseModel):
    view_element_id: Optional[str]
    view_element_name: Optional[str]
    key_attributes: Optional[Dict]


class ViewElementsGetRequestEventModel(RequestEventModel):
    queryStringParameters: ViewElementSerializer

    @validator("queryStringParameters", pre=True, always=True)
    def set_queryStringParameters(cls, queryStringParameters):
        return queryStringParameters or {}


class ViewElementsGetResponse(ResponseModel):
    view_elements: List[ViewElementSerializer]
