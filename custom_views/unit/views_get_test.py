import json

from controllers.views import ViewsGet
from controllers.views.schemas.ViewsGet import ViewsListResponse

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock


USER_ID = get_unit_test_user()


def test_get_list_success_views():
    event = event_object_mock(USER_ID)
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    ViewsListResponse(**json.loads(response["body"]))


def test_get_list_active_views():
    event = event_object_mock(USER_ID, query_parameters={"is_active": True})
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    ViewsListResponse(**json.loads(response["body"]))


def test_get_list_invalid_params():
    event = event_object_mock(USER_ID, query_parameters={"is_active": "a"})
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 500
