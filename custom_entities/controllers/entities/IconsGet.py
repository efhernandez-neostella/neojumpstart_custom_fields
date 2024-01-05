from core_utils.functions import UUID, lambda_decorator, tenant_setup
from utils.auth.permissions import Permission
from entities_utils import get_icons


def _check_permissions(user_id: str, tenant_id: str) -> None:
    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="entity_configuration",
        subcomponent="entity",
        action="can_create",
    )
    permission.check_permissions()


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    _check_permissions(user_id, tenant_id)
    icons = get_icons()
    return (200, {"correlation_id": UUID, "result": "Get icons success", "icons": icons})
