import pytest
from controllers import CustomObjectPut

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()
PATH_PARAMETERS = {"entity_name": "cars", "id": "4a41242f-64c2-4c5d-bf5f-ba34ce1ed666"}
BODY = {
    "cars_id": "4a41242f-64c2-4c5d-bf5f-ba34ce1ed666",
    "cf_name": "Islero Updated",
    "cf_is_active": False,
    "cf_purchased_date": "12/06/2023",
    "cf_tire": {"picklist_id": "f775b122-efe2-4e44-8cfe-6c0832202d41", "name": "Bridgestone"},
    "cf_brakes": [
        {"picklist_id": "2829d9ef-91b3-4337-9f6f-59aacc4bb754", "name": "Brembo"},
        {"picklist_id": "41bb46f0-6f0e-4c96-9e57-cb79f3cf7b10", "name": "Tammington"},
    ],
    "cf_color": [
        {"colors_id": "01f9f2cf-f17d-4dba-afcf-d3f159b6f7df", "cf_name": "Red"},
        {"colors_id": "faa317ea-0578-4d13-82de-473d6adf9555", "cf_name": "Blue"},
    ],
    "cf_toys": [
        {"picklist_id": "5486c077-c668-4195-ab8a-135e5f3e8008", "name": "Slinky"},
        {"picklist_id": "68e2fb4f-afdf-4586-8939-9778dee48543", "name": "Mr. Potato"},
    ],
}


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_NAME,
        path_parameters=PATH_PARAMETERS,
        body=BODY,
    )


def test_object_put_autocomplete_request_success_response(apigw_event):
    """Test successful operation of PUT custom object endpoint"""
    response = CustomObjectPut.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
