import json

import pytest
from controllers.fields import FieldPost

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_NAME = get_unit_test_user()


BODY = {
    "column_name": "crayon color",
    "name": "Crayon Color",
    "parent_object_id": "63c97646-8dd1-4e0f-9ebb-01874a8c161f",
    "input": "autocomplete",
    "format": None,
    "description": "Crayon color description",
    "acceptable_values": [],
    "data_source": {
        "property_entity_id": "6addb01a-ddfd-4be2-bb5b-a0b72956174a",
        "property_field_id": "633bff49-1694-44a9-bbf8-a9ce4e9e516f",
    },
    "is_required": False,
    "properties": {
        "is_multiple": True,
        "is_required": True,
    },
    "height": 1,
    "width": 6,
}


BODY_2 = {
    "is_active": True,
    "column_name": "tire",
    "name": "Tire",
    "parent_object_id": "6495da79-153a-478b-b5b6-5cef7aec4528",
    "input": "checkbox",
    "format": None,
    "is_required": True,
    "description": "Tires brands",
    "width": 6,
    "height": 1,
    "acceptable_values": [
        {"name": "Bridgestone", "is_active": True, "order": 0},
        {"name": "Goodyear", "is_active": True, "order": 1},
        {"name": "Michelin", "is_active": True, "order": 2},
    ],
    "data_source": {},
    "properties": {"required": True, "is_picklist": True, "is_multiple": True},
}


@pytest.fixture(params=[BODY, BODY_2], ids=["BODY", "BODY_2"])
def apigw_event(request):
    """Generates API GW Event"""

    return event_object_mock(
        user_id=USER_NAME,
        body=request.param,
    )


def test_entity_post_picklist_type_field_success_response(apigw_event):
    """
    Test successful operation of POST entity endpoint
    with a checkbox type field as input with picklist property
    """
    response = FieldPost.lambda_handler(apigw_event, "")
    body = json.loads(apigw_event["body"])
    if body == BODY_2:
        assert response["statusCode"] == 400
    else:
        assert response["statusCode"] == 200


def test_entity_post_autocomplete_type_field_success_response(apigw_event):
    """
    Test successful operation of POST entity endpoint
    with a a autocomplete type field as input
    """
    response = FieldPost.lambda_handler(apigw_event, "")
    body = json.loads(apigw_event["body"])
    if body == BODY:
        assert response["statusCode"] == 200


def test_entity_post_checkbox_type_field_success_response(apigw_event):
    """
    Test successful operation of POST entity endpoint
    with a checkbox type field as input
    """
    # json.loads(apigw_event["body"].update({"input": "checkbox"}))
    body = json.loads(apigw_event["body"])
    body.update({"input": "checkbox"})
    apigw_event["body"] = json.dumps(body)
    response = FieldPost.lambda_handler(apigw_event, "")
    print(response)
    assert response["statusCode"] == 200


def test_entity_post_radio_type_field_success_response(apigw_event):
    """
    Test successful operation of POST entity endpoint
    with a radio button type field as input
    """
    body = json.loads(apigw_event["body"])
    body.update({"input": "radio"})
    apigw_event["body"] = json.dumps(body)
    response = FieldPost.lambda_handler(apigw_event, "")
    print(response)
    assert response["statusCode"] == 200
