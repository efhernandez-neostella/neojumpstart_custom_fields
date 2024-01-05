import json

from controllers.views import ViewElementsGet
from controllers.views.schemas.ViewElements import ViewElementsGetResponse

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock


USER_ID = get_unit_test_user()


def test_get_list_success_view_elements():
    event = event_object_mock(USER_ID)
    response = ViewElementsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    ViewElementsGetResponse(**json.loads(response["body"]))


def test_get_list_invalid_params():
    event = event_object_mock(USER_ID, query_parameters={"view_element_id": True})
    response = ViewElementsGet.lambda_handler(event, "")
    assert response["statusCode"] == 500
