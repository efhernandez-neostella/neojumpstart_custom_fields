import pytest
from controllers.entities import EntityPut

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_NAME,
        path_parameters={"entity_id": "75db96e8-01b2-486f-82ff-7bf6219a1f98"},
        body={
            "name": "Test entity name 1",
            "database_name": "test_entity_name_1",
            "translation_reference": "test_entity_name_1",
            "api_name": "test-entity-name-1",
            "properties": {
                "can_edit_fields": True,
                "can_edit_views": True,
                "can_remove": True,
                "can_soft_delete": True,
                "has_file_storage": True,
                "is_child": True,
                "parent_entity_id": "8ec23fb2-fb7e-4dee-9198-bbac0b9a9ac6",
            },
            "icon_data": {"icon_name": "ad", "icon_class": "ti-ad"},
        },
    )


def test_entity_put_success_response(apigw_event):
    """Test successful operation of PUT entity endpoint"""
    response = EntityPut.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
