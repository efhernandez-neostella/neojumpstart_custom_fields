import pytest
from controllers.entities import EntityPost

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_NAME,
        body={
            "name": "Entity test 5",
            "database_name": "entity-test-database-5",
            "translation_reference": "Entity test translation reference 5",
            "api_name": "Entity test 5",
            "properties": {
                "can_edit_fields": True,
                "can_edit_views": True,
                "can_remove": True,
                "can_soft_delete": True,
                "has_file_storage": True,
            },
            "icon_data": {"icon_name": "ad", "icon_class": "ti-ad"},
        },
    )


def test_entity_post_success_response(apigw_event):
    """Test successful operation of POST entity endpoint"""
    response = EntityPost.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
