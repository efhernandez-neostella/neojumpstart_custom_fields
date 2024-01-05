import json

from models.FieldType import FieldTypeModel

from core_utils.functions import UUID, lambda_decorator, tenant_setup
from utils.auth.permissions import Permission
from utils.common import handle_payload_error


def _check_permissions(user_id: str, tenant_id: str) -> None:
    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="object_configurations",
        subcomponent="fields",
        action="can_read",
    )
    permission.check_permissions()


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event['user_id'], event['tenant_id']
    _check_permissions(user_id, tenant_id)
    field_types = FieldTypeModel(user_id, tenant_id).perform_get_list()
    response = (200, {"field_types": field_types})
    return response
