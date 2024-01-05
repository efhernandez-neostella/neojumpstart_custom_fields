import json

from controllers.views import ViewsGet, ViewsPut
from controllers.views.schemas.ViewsGet import ViewsListResponse
from controllers.views.schemas.ViewsPut import ViewPutResponse

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()

JSON_CONTENT = {
    "cards": [
        {
            "buttons": {
                "header": [],
                "former": [],
                "latter": [
                    {
                        "variant": "contained",
                        "endpoint": "/custom/planes",
                        "text": "Create",
                        "type": "save",
                        "url": "/planes",
                    }
                ],
            },
            "title": "New Form",
            "card_type": "form",
            "card_id": "ec493b3f-8ca5-4c4c-a84a-f9bf5b48dc37",
            "properties": {"xs": "12"},
            "sections": [
                {
                    "section_type": "form",
                    "buttons": None,
                    "section_id": "00f9cfb3-1761-4c1b-b10f-c374b2a8b1b7",
                    "title": "New section",
                    "fields": [
                        {
                            "custom_field_id": "bc718a3a-72ab-45c7-9a86-794946911286",
                            "is_active": True,
                            "column_name": "cf_name",
                            "name": "Name",
                            "parent_object_id": "3221bcd5-160a-47b3-85b0-9cb17ba92ed1",
                            "parent_object_name": "Planes",
                            "input": "text",
                            "data_type": "varchar",
                            "format": None,
                            "is_required": True,
                            "description": "Record name default field",
                            "width": 6,
                            "height": 1,
                            "acceptable_values": [],
                            "data_source": {},
                            "tenant_id": "96e27c11-bbaf-4b47-9c04-537b1ec12ea7",
                            "properties": {
                                "is_required": True,
                                "default_field": True,
                                "custom_field_id": "bc718a3a-72ab-45c7-9a86-794946911286",
                                "tenant_id": "96e27c11-bbaf-4b47-9c04-537b1ec12ea7",
                                "parent_object_id": "3221bcd5-160a-47b3-85b0-9cb17ba92ed1",
                                "label": "Name",
                                "is_label": True,
                                "max_characters": 250,
                                "is_special_characters": True,
                                "is_max_characters": True,
                            },
                        },
                        {
                            "custom_field_id": "344f8002-b6d5-409a-8094-d188cf6c8b20",
                            "is_active": True,
                            "column_name": "cf_is_active",
                            "name": "Is active",
                            "parent_object_id": "3221bcd5-160a-47b3-85b0-9cb17ba92ed1",
                            "parent_object_name": "Planes",
                            "input": "toggle",
                            "data_type": "boolean",
                            "format": None,
                            "is_required": True,
                            "description": "Is active default field",
                            "width": 6,
                            "height": 1,
                            "acceptable_values": [],
                            "data_source": {},
                            "tenant_id": "96e27c11-bbaf-4b47-9c04-537b1ec12ea7",
                            "properties": {
                                "default_field": True,
                                "custom_field_id": "344f8002-b6d5-409a-8094-d188cf6c8b20",
                                "default_value": True,
                                "tenant_id": "96e27c11-bbaf-4b47-9c04-537b1ec12ea7",
                                "parent_object_id": "3221bcd5-160a-47b3-85b0-9cb17ba92ed1",
                                "label": "Is Active",
                                "is_label": True,
                                "is_default_value": True,
                            },
                        },
                    ],
                    "properties": {"xs": 12},
                }
            ],
        }
    ]
}


def test_put_success_views():
    event = event_object_mock(USER_ID)
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    views = ViewsListResponse(**json.loads(response["body"]))
    view_id = views.views[0].view_id
    if view_id:
        body = {
            "view_id": view_id,
            "view_name": "users Testing",
            "view_endpoint": "/test",
            "view_route": "testing",
            "is_active": True,
            "json_content": JSON_CONTENT,
        }
        event = event_object_mock(USER_ID, body=body)
        response = ViewsPut.lambda_handler(event, "")
        assert response["statusCode"] == 200
        ViewPutResponse(**json.loads(response["body"]))


def test_put_view_with_wrong_type_button_header():
    event = event_object_mock(USER_ID)
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    views = ViewsListResponse(**json.loads(response["body"]))
    view_id = views.views[0].view_id
    if view_id:
        body = {
            "view_id": view_id,
            "view_name": "Test view",
            "view_endpoint": "/test",
            "view_route": "testing",
            "is_active": True,
            "json_content": {
                "cards": [
                    {
                        "buttons": {
                            "header": [
                                {
                                    "variant": "contained",
                                    "endpoint": "/custom/cars",
                                    "text": "Update",
                                    "type": "edit",
                                    "url": "/cars/{id}",
                                }
                            ],
                            "former": [],
                            "latter": [],
                        },
                        "title": "New Form",
                        "card_type": "form",
                        "card_id": "17baea01-8302-4f7a-bc24-ae2fd517c8b9",
                        "properties": {"xs": "12"},
                        "sections": [],
                    }
                ]
            },
        }
        event = event_object_mock(USER_ID, body=body)
        response = ViewsPut.lambda_handler(event, "")
        assert response["statusCode"] == 404


def test_put_invalid_params():
    event = event_object_mock(USER_ID)
    response = ViewsGet.lambda_handler(event, "")
    assert response["statusCode"] == 200
    views = ViewsListResponse(**json.loads(response["body"]))
    view_id = views.views[0].view_id
    if view_id:
        body = {
            "view_id": view_id,
            "view_name": {},
            "view_endpoint": [4343],
            "view_route": 4334,
            "is_active": "True",
        }
        event = event_object_mock(USER_ID, body=body)
        response = ViewsPut.lambda_handler(event, "")
        assert response["statusCode"] == 500
