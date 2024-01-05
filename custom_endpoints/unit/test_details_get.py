import pytest
from controllers import CustomDetailsGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event

    The following path parameters are only functional in dev environment.
    """

    return event_object_mock(
        user_id=USER_NAME,
        path_parameters={"entity_name": "accounts", "id": "4b28590a-e28f-459a-a391-7a4d1e05b3ce"},
    )


def test_entities_get_success_response(apigw_event):
    """Test successful operation of GET custom details endpoint"""

    response = CustomDetailsGet.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
