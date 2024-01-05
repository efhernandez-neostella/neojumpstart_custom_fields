from aws_lambda_powertools.utilities.parser import event_parser
from aws_lambda_powertools.utilities.typing import LambdaContext
from controllers.views.schemas.ViewGet import (ViewGetRequestEventModel,
                                               ViewGetResponse)
from models.View import ViewModel
from process.views.ViewGet import ViewGet

from core_utils.functions import lambda_decorator, tenant_setup
# from utils.auth.permissions import Permission
from utils.common import get_user_timezone


@tenant_setup
@lambda_decorator
@event_parser(model=ViewGetRequestEventModel)
def lambda_handler(event: ViewGetRequestEventModel, context: LambdaContext):
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
    view_object = ViewModel(**event.pathParameters.dict(exclude_unset=True))
    view_object.time_zone = get_user_timezone(user_id=event.user_id)

    # Assign audit values to view model
    view_object.tenant_id = event.tenant_id

    # Specific get process
    view_get_process = ViewGet(view_object)
    response = view_get_process.perform()

    # Validate response
    ViewGetResponse(**response[1])

    return response
