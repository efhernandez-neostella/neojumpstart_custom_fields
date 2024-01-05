from aws_lambda_powertools.utilities.parser import BaseModel
from aws_lambda_powertools.utilities.parser.models import APIGatewayProxyEventModel

from typing import Optional
from datetime import datetime


class CreationUserAudit(BaseModel):
    created_at: Optional[datetime]
    created_by_user_id: Optional[str]
    created_by_user_name: Optional[str]


class UpdateUserAudit(BaseModel):
    updated_at: Optional[datetime]
    updated_by_user_id: Optional[str]
    updated_by_user_name: Optional[str]


class AuditData(BaseModel):
    created: CreationUserAudit
    updated: UpdateUserAudit


class ResponseModel(BaseModel):
    correlation_id: str
    result: str


class RequestEventModel(APIGatewayProxyEventModel):
    user_id: str
    tenant_id: str
