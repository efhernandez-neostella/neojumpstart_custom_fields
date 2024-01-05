from controllers.fields import FieldGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


def test_create_field():
    event = event_object_mock(
        USER_ID, path_parameters={"custom_field_id": "fcde0caa-4f71-4c30-8ca9-802f81b3ed02"}
    )
    response = FieldGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
