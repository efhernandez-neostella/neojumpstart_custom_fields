import pytest
from controllers.fields import FieldPut

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()
PATH_PARAMETERS = {"custom_field_id": "4b569470-0a81-4a66-bea0-499db6463547"}
BODY = {
    "custom_field_id": "4b569470-0a81-4a66-bea0-499db6463547",
    "is_active": True,
    "column_name": "cf_tires",
    "name": "Tires",
    "parent_object_id": "ee03711b-067a-4cd7-adfa-9968c8fa19f6",
    "input": "autocomplete",
    "format": None,
    "is_required": False,
    "description": "Truck tire brand UPDATED",
    "width": 6,
    "height": 1,
    "acceptable_values": [
        {
            "picklist_id": "6a5879b4-8347-4a02-becf-9c49835f4fff",
            "name": "Yokohama",
            "is_active": False,
            "order": 0,
        },
        {
            "picklist_id": "e6be8ae0-2546-4f35-98eb-41e63cf73962",
            "name": "Michelin",
            "is_active": True,
            "order": 1,
        },
        {
            "picklist_id": "eea8b3be-5344-44ee-ac9c-c216f7070ab0",
            "name": "Continental",
            "is_active": True,
            "order": 2,
        },
        {
            "picklist_id": "5bbade35-057a-438e-8268-a92d69ab432e",
            "name": "Bridgestone",
            "is_active": False,
            "order": 3,
        },
        {
            "picklist_id": "c7f4492e-286a-4f7b-a786-4aec5b452313",
            "name": "Pirelli",
            "is_active": False,
            "order": 4,
        },
        {
            "picklist_id": "ff84040c-7a81-4325-b485-a6b047430e24",
            "name": "Dunlop",
            "is_active": True,
            "order": 5,
        },
        {
            "picklist_id": "2e4013d3-cd31-49cf-8d47-97e6f1df3bd6",
            "name": "Goodyear",
            "is_active": True,
            "order": 6,
        },
        {
            "picklist_id": "c8782dd1-ac2c-4a2c-976e-1f7ccb180c65",
            "name": "Hankook",
            "is_active": True,
            "order": 7,
        },
        {
            "name": "Cooper",
            "is_active": True,
            "order": 8,
        },
        {
            "name": "Firestone",
            "is_active": True,
            "order": 9,
        },
        {
            "name": "Toyo",
            "is_active": True,
            "order": 10,
        },
    ],
    "data_source": {},
    "properties": {
        "label": "Truck tire brand",
        "is_label": True,
        "is_picklist": True,
        "is_required": False,
        "is_multiple": False,
    },
}


@pytest.fixture()
def apigw_event():
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_NAME,
        path_parameters=PATH_PARAMETERS,
        body=BODY,
    )


def test_entity_put_field_success_response(apigw_event):
    """Test successful operation of PUT field endpoint"""
    response = FieldPut.lambda_handler(apigw_event, "")
    assert response["statusCode"] == 200
