import pytest
from controllers import CustomObjectPost

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()
PATH_PARAMETERS = {"entity_name": "cars"}
BODY = {
    "cf_name": "Miura",
    "cf_is_active": True,
    "cf_purchased_date": "12/07/2023",
    "cf_tire": {"picklist_id": "2aced275-bf60-4383-98aa-e665a9416631", "name": "Michelin"},
    "cf_brakes": [
        {"picklist_id": "2829d9ef-91b3-4337-9f6f-59aacc4bb754", "name": "Brembo"},
        {"picklist_id": "41bb46f0-6f0e-4c96-9e57-cb79f3cf7b10", "name": "Tammington"},
        {"picklist_id": "3ce1b30a-9d18-4378-8131-43848dd7384e", "name": "Ferodo"},
    ],
    "cf_color": [
        {"colors_id": "62c6c05f-fc14-4f00-bf40-d40096afa57f", "cf_name": "White"},
        {"colors_id": "01f9f2cf-f17d-4dba-afcf-d3f159b6f7df", "cf_name": "Red"},
        {"colors_id": "faa317ea-0578-4d13-82de-473d6adf9555", "cf_name": "Blue"},
    ],
    "cf_toys": [
        {"picklist_id": "99fe1de3-251e-46ad-baff-bca2766f6a39", "name": "Woody"},
        {"picklist_id": "68e2fb4f-afdf-4586-8939-9778dee48543", "name": "Mr. Potato"},
    ],
}


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(user_id=USER_NAME, path_parameters=PATH_PARAMETERS, body=BODY)


def test_object_post_success_response(apigw_event):
    """Test successful operation of POST custom object endpoint"""
    response = CustomObjectPost.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
