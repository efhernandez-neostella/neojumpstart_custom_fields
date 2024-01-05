from controllers.fields import FieldsGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


def test_create_field():
    event = event_object_mock(
        USER_ID,
        query_parameters={
            "is_active": True,
            "parent_object_id": "6495da79-153a-478b-b5b6-5cef7aec4528",
        },
    )
    response = FieldsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
