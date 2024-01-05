import json

import pytest
from controllers.views import ViewGet, ViewsGet
from controllers.views.schemas.ViewGet import ViewGetResponse
from controllers.views.schemas.ViewsGet import ViewsListResponse

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_ID, path_parameters={"view_id": "5536ffa1-9dbd-4a7d-80ee-6b2a7bbdd150"}
    )


def test_view_get_success_response(apigw_event):
    """Test successful operation of GET view endpoint"""

    response = ViewGet.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
    ViewGetResponse(**json.loads(response["body"]))


def test_get_success_views():
    event = event_object_mock(USER_ID)
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    views = ViewsListResponse(**json.loads(response["body"]))
    view_id = views.views[0].view_id
    if view_id:
        event = event_object_mock(USER_ID, path_parameters={"view_id": view_id})
        response = ViewGet.lambda_handler(event, "")
        assert response["statusCode"] == 200
        ViewGetResponse(**json.loads(response["body"]))


def test_get_invalid_params():
    event = event_object_mock(USER_ID, path_parameters={"view_id": True})
    response = ViewGet.lambda_handler(event, "")
    assert response["statusCode"] == 400
