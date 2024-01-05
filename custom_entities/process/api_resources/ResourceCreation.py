import os

from models.Resource import ResourceModel

from core_utils.functions import LAMBDA_CLIENT, REGION_NAME
from utils.exceptions.controller_exceptions import ControllerException

STAGE = os.getenv("STAGE", None)
REST_API_ID = os.getenv("REST_API_ID", None)
REST_API_ROOT_RESOURCE_ID = os.getenv("REST_API_ROOT_RESOURCE_ID", None)
AUTHORIZER_ID = os.getenv("AUTHORIZER_ID", None)


class ResourceCreation:
    def __init__(self, resource_object: "ResourceModel", view_type: str, payload: dict):
        self.resource_object = resource_object
        self.view_type = view_type
        self.payload = payload
        self.api_id = REST_API_ID

    def perform(self):
        self._check_custom_parent_resource()
        view_setup_functions = {
            "list": self._list_get_endpoint_setup,
            "create": self._post_endpoint_setup,
            "update": self._put_endpoint_setup,
        }
        if self.view_type in view_setup_functions:
            return view_setup_functions[self.view_type]()
        else:
            raise ControllerException(
                400,
                {
                    "error": f"Error creating {self.view_type} endpoint resource.",
                    "code": "CustomEntities.ResourceCreationError",
                },
            )

    def _common_endpoint_setup(
        self, http_method: str, child_resource: str, lambda_name: str, details_resource: str = ""
    ):
        parent_resource = "specific"
        path = f"/{parent_resource}/{child_resource}"
        if details_resource:
            parent_resource = f"{parent_resource}/{child_resource}"
            path = f"/{parent_resource}/{details_resource}"
            child_resource = details_resource
        lambda_function_name = f"apptemplatecustomendpoints-{STAGE}-Custom{lambda_name}"
        statement_id = f"{lambda_function_name}-{self.payload['entity_name']}"
        endpoint_id, endpoint_path = self._endpoint_creation(
            http_method,
            path,
            parent_resource,
            child_resource,
            lambda_function_name,
            statement_id,
        )
        return endpoint_id, endpoint_path

    def _get_endpoint_setup(self):
        endpoint_id, endpoint_path = self._common_endpoint_setup(
            "GET", self.payload["entity_name"], "DetailsGet", "{id}"
        )
        self._create_options_endpoint(endpoint_id)
        return endpoint_path

    def _list_get_endpoint_setup(self):
        endpoint_id, endpoint_path = self._common_endpoint_setup(
            "GET", self.payload["entity_name"], "ListGet", ""
        )
        self._create_options_endpoint(endpoint_id)
        return endpoint_path

    def _post_endpoint_setup(self):
        endpoint_id, endpoint_path = self._common_endpoint_setup(
            "POST", self.payload["entity_name"], "ObjectPost", ""
        )
        return endpoint_path

    def _put_endpoint_setup(self):
        self._get_endpoint_setup()
        endpoint_id, endpoint_path = self._common_endpoint_setup(
            "PUT", self.payload["entity_name"], "ObjectPut", "{id}"
        )
        return endpoint_path

    def _endpoint_creation(
        self,
        http_method: str,
        path: str,
        parent_resource: str,
        child_resource: str,
        lambda_function_name: str,
        statement_id: str,
    ):
        parent_resource_path = f"/{parent_resource}"
        valid, resource_id = self._check_if_resource_exist(path)

        if not valid:
            parent_resource_id = self.resource_object.get_specific_resource(parent_resource_path)
            resource_id = self.resource_object.create_resource(parent_resource_id, child_resource)

        lambda_function_arn = LAMBDA_CLIENT.get_function(FunctionName=lambda_function_name)[
            "Configuration"
        ]["FunctionArn"]

        service_path = "lambda:path/2015-03-31/functions"
        resource_identifier = (
            f"arn:aws:apigateway:{REGION_NAME}:{service_path}/{lambda_function_arn}/invocations"
        )

        self.resource_object.method_request(resource_id, http_method)
        self.resource_object.integration_request(resource_id, http_method, resource_identifier)
        self.resource_object.method_response(resource_id, http_method)
        self.resource_object.integration_response(resource_id, http_method)
        self.resource_object.lambda_invocation_permission(
            http_method, path, statement_id, lambda_function_name
        )
        return resource_id, path

    def _create_options_endpoint(self, resource_id: str):
        http_method = "OPTIONS"
        self.resource_object.method_request_options(resource_id, http_method)
        self.resource_object.integration_request_options(resource_id, http_method)
        self.resource_object.method_response(resource_id, http_method)
        self.resource_object.integration_response(resource_id, http_method)

    def _check_custom_parent_resource(self):
        custom_parent_resource = "specific"
        resource_path = f"/{custom_parent_resource}"
        root_resource_id = REST_API_ROOT_RESOURCE_ID
        valid, resource_id = self._check_if_resource_exist(resource_path)
        if not valid:
            self.resource_object.create_resource(root_resource_id, custom_parent_resource)

    def _check_if_resource_exist(self, path: str):
        resources = self.resource_object.get_resources()
        if resource_id := self.get_resource_id(path, resources):
            return True, resource_id
        return False, ""

    def get_resource_id(self, path: str, resources: list) -> str:
        for resource in resources:
            if resource.get("path") == path:
                path_id = resource.get("id")
                return path_id
        return ""
