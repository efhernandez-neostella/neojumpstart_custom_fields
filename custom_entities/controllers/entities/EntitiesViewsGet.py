from models.Entity import EntityModel
from process.entities.EntitiesViewsGet import EntitiesViewsGet

from core_utils.functions import lambda_decorator, tenant_setup

# from utils.auth.permissions import Permission


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = event["queryStringParameters"]
    if payload is None:
        payload = {}

    # permission = Permission(
    #     user_id=user_id,
    #     tenant_id=tenant_id,
    #     event_uuid=UUID,
    #     module="admin",
    #     component_name="entity_configuration",
    #     subcomponent="entity",
    #     action="can_read",
    # )
    # permission.check_permissions()

    entity_object = EntityModel(user_id, tenant_id)
    entities_get_process = EntitiesViewsGet(entity_object=entity_object, payload=payload)
    response = entities_get_process.perform()

    return response
