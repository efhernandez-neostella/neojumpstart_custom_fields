import pytest
from controllers.entities import EntityGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_NAME,
        path_parameters={"entity_id": "6495da79-153a-478b-b5b6-5cef7aec4528"},
    )


def test_entities_get_success_response(apigw_event):
    """Test successful operation of POST entity endpoint"""

    response = EntityGet.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
