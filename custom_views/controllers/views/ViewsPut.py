from aws_lambda_powertools.utilities.parser import (
    event_parser,
)

from aws_lambda_powertools.utilities.typing import LambdaContext
from process.views.ViewsPut import ViewsPut
from models.View import ViewModel

from core_utils.functions import lambda_decorator, tenant_setup
from controllers.views.schemas.ViewsPut import (
    ViewPutRequestEventModel,
    ViewPutResponse,
)

# from utils.auth.permissions import Permission


@tenant_setup
@lambda_decorator
@event_parser(model=ViewPutRequestEventModel)
def lambda_handler(event: ViewPutRequestEventModel, context: LambdaContext):
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

    view_object = ViewModel(**event.body.dict(exclude_unset=True))

    # Assign audit values to view model
    view_object.tenant_id = event.tenant_id
    view_object.updated_by = event.user_id

    # Edit process
    views_put_process = ViewsPut(view_object, event.body.json_content)
    response = views_put_process.perform()

    # Response validation
    ViewPutResponse(**response[1])

    return response
