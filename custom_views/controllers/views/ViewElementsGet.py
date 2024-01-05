from aws_lambda_powertools.utilities.parser import (
    event_parser,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from models.ViewElement import ViewElementModel

from core_utils.functions import lambda_decorator, tenant_setup, UUID

# from utils.auth.permissions import Permission
from controllers.views.schemas.ViewElements import (
    ViewElementsGetRequestEventModel,
    ViewElementsGetResponse,
)


@tenant_setup
@lambda_decorator
@event_parser(model=ViewElementsGetRequestEventModel)
def lambda_handler(event: ViewElementsGetRequestEventModel, context: LambdaContext):
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
    view_element_object = ViewElementModel(**event.queryStringParameters.dict(exclude_unset=True))

    # Assign audit values to view model

    response = (
        200,
        {
            "correlation_id": UUID,
            "view_elements": view_element_object.get_view_elements(),
            "result": "Get view elements success",
        },
    )

    # Validate response
    ViewElementsGetResponse(**response[1])

    return response
