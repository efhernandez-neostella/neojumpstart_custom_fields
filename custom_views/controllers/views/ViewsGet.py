from aws_lambda_powertools.utilities.parser import (
    event_parser,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from process.views.ViewsGet import ViewsGet
from models.View import ViewModel

from core_utils.functions import lambda_decorator, tenant_setup

# from utils.auth.permissions import Permission
from utils.common import get_user_timezone
from controllers.views.schemas.ViewsGet import ViewListRequestEventModel, ViewsListResponse


@tenant_setup
@lambda_decorator
@event_parser(model=ViewListRequestEventModel)
def lambda_handler(event: ViewListRequestEventModel, context: LambdaContext):
    # TODO: When permission is defined
    # permission = Permission(
    #     user_id=event.user_id,
    #     tenant_id=event.tenant_id,
    #     event_uuid=UUID,
    #     module="admin",
    #     component_name="entity_configuration",
    #     subcomponent="entity",
    #     action="can_read",
    # )
    # permission.check_permissions()

    # User time zone
    view_object = ViewModel(**event.queryStringParameters.dict(exclude_unset=True))
    view_object.time_zone = get_user_timezone(user_id=event.user_id)

    # Assign audit values to view model
    view_object.tenant_id = event.tenant_id

    # List get process
    views_get_process = ViewsGet(view_object)
    response = views_get_process.perform()

    # Validate response
    ViewsListResponse(**response[1])

    return response
