import pytest
from controllers.entities import EntitiesViewsGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(user_id=USER_NAME)


def test_entities_get_success_response(apigw_event):
    """Test successful operation of Get entities views endpoint"""

    ret = EntitiesViewsGet.lambda_handler(apigw_event, "")
    assert ret["statusCode"] == 200
