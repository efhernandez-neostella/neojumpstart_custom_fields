import os

import boto3

from core_utils.functions import LAMBDA_CLIENT, REGION_NAME

API_GATEWAY_CLIENT = boto3.client("apigateway")
IAM_CLIENT = boto3.client("iam")
STS_CLIENT = boto3.client("sts")
ACCOUNT_ID = STS_CLIENT.get_caller_identity()["Account"]

STAGE = os.getenv("STAGE", None)
REST_API_ID = os.getenv("REST_API_ID", None)
REST_API_ROOT_RESOURCE_ID = os.getenv("REST_API_ROOT_RESOURCE_ID", None)
AUTHORIZER_ID = os.getenv("AUTHORIZER_ID", None)


class ResourceModel:
    def __init__(self, user_id: str, tenant_id: str):
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.api_id = REST_API_ID

    def get_resources(self):
        resources = API_GATEWAY_CLIENT.get_resources(
            restApiId=self.api_id,
            limit=500,
        )
        return resources.get("items")

    def get_specific_resource(self, parent_path):
        resources = self.get_resources()
        for resource in resources:
            if resource.get("path") == parent_path:
                parent_path_id = resource.get("id")
        return parent_path_id

    def create_resource(self, resource_id: str, path: str):
        resource = API_GATEWAY_CLIENT.create_resource(
            restApiId=self.api_id, parentId=resource_id, pathPart=path
        )
        resource_id = resource["id"]
        return resource_id

    def method_request(self, resource_id: str, http_method: str):
        api_method = API_GATEWAY_CLIENT.put_method(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            authorizationType="COGNITO_USER_POOLS",
            authorizerId=AUTHORIZER_ID,
            authorizationScopes=["aws.cognito.signin.user.admin", "apiauthidentifier/json.read"],
        )
        return api_method

    def integration_request(self, resource_id: str, http_method: str, resource_identifier: str):
        api_integration = API_GATEWAY_CLIENT.put_integration(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            integrationHttpMethod="POST",
            type="AWS_PROXY",
            uri=resource_identifier,
        )
        return api_integration

    def method_response(self, resource_id: str, http_method: str):
        put_method_response = API_GATEWAY_CLIENT.put_method_response(
            restApiId=self.api_id, resourceId=resource_id, httpMethod=http_method, statusCode="200"
        )
        return put_method_response

    def integration_response(self, resource_id: str, http_method: str):
        api_integration_response = API_GATEWAY_CLIENT.put_integration_response(
            restApiId=self.api_id, resourceId=resource_id, httpMethod=http_method, statusCode="200"
        )
        return api_integration_response

    def api_deployment(self):
        api_deployment = API_GATEWAY_CLIENT.create_deployment(
            restApiId=self.api_id, stageName=STAGE
        )
        return api_deployment

    def lambda_invocation_permission(
        self, http_method: str, path: str, statement_id: str, lambda_function_name
    ):
        resource_arn = (
            f"arn:aws:execute-api:{REGION_NAME}:{ACCOUNT_ID}:{self.api_id}/*/{http_method}{path}"
        )
        api_permission = LAMBDA_CLIENT.add_permission(
            FunctionName=lambda_function_name,
            StatementId=f"apigateway-lambda-{statement_id}",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=resource_arn,
        )
        return api_permission

    def method_request_options(self, resource_id: str, http_method: str):
        api_method = API_GATEWAY_CLIENT.put_method(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            authorizationType="NONE",
        )
        return api_method

    def integration_request_options(self, resource_id: str, http_method: str):
        api_integration = API_GATEWAY_CLIENT.put_integration(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            type="MOCK",
        )
        return api_integration
