import pytest
from controllers.fields import FieldTypesGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(user_id=USER_NAME)


def test_field_types_get_success_response(apigw_event):
    """Test successful operation of GET field types endpoint"""

    ret = FieldTypesGet.lambda_handler(apigw_event, "")
    print(ret)
    assert ret["statusCode"] == 600
