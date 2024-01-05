from typing import Dict, List, Optional

from aws_lambda_powertools.utilities.parser import BaseModel
from pydantic import Extra

from utils.models import AuditData


class ViewEntity(BaseModel):
    entity_id: str
    name: str


class FieldModel(BaseModel, extra=Extra.allow):
    custom_field_id: str


class Button(BaseModel, extra=Extra.allow):
    variant: Optional[str]
    text: Optional[str]
    type: Optional[str]
    endpoint: Optional[str]
    url: Optional[str]


class ButtonType(BaseModel, extra=Extra.allow):
    header: Optional[List[Button]]
    former: Optional[List[Button]]
    latter: Optional[List[Button]]


class Section(BaseModel, extra=Extra.allow):
    buttons: Optional[ButtonType]
    section_id: Optional[str]
    title: Optional[str]
    properties: Optional[Dict]
    fields: Optional[List[FieldModel]]


class Card(BaseModel, extra=Extra.allow):
    card_id: Optional[str]
    title: Optional[str]
    buttons: Optional[ButtonType]
    card_type: Optional[str]
    properties: Optional[Dict]
    sections: Optional[List[Section]]


class JsonContent(BaseModel):
    cards: Optional[List[Card]]


class ViewSerializer(BaseModel):
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
    audit_data: Optional[AuditData]
    json_content: Optional[JsonContent]
    entity: Optional[ViewEntity]
