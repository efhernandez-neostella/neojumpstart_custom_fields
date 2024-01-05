from json import dumps
from datetime import datetime


def create_account_body(
    name: str = "Sample",
    notes: str = """This is an example Account for you to get you started! 
    Accounts give you a holistic view of your client projects, hours billed and more 
    (especially as you turn on features). Everything starts with creating your Account.""",
) -> dict:
    return {"name": name, "notes": notes}


def create_project_body(
    account_custom_id: str,
    name: str = "Sample",
    description: str = """This is your first example project associated to your first Account. 
    Projects are nested under Accounts to aggregate their information for insights 
    across an entire Account. You’re now ready to start tracking time! """,
) -> dict:
    return {"name": name, "description": description, "account_custom_id": account_custom_id}


def create_time_entry_body(
    account_custom_id: str,
    project_id: str,
    user_id: str,
    time_entry_date: str,
    time_spent_minutes: int = 10,
    external_note: str = "",
    internal_note: str = "",
    task_id: str = None,
    position_id: str = None,
) -> str:
    return {
        "user_id": user_id,
        "project_id": project_id,
        "account_custom_id": account_custom_id,
        "time_spent_minutes": time_spent_minutes,
        "time_entry_date": time_entry_date,
        "external_note": external_note,
        "internal_note": internal_note,
        "task_id": task_id,
        "position_id": position_id,
    }


def create_grid_state_body(
    columns: dict = {"account": True, "project": True, "position": True, "task": True}
) -> str:
    return {"columns": columns}


def event_object_mock(
    user_id: str = "", body: dict = {}, path_parameters: dict = {}, query_parameters: dict = {}
) -> dict:
    return {
        "body": dumps(body),
        "resource": "/{proxy+}",
        "requestContext": {
            "authorizer": {
                "claims": {"scope": "aws.cognito.signin.user.admin", "username": user_id}
            },
            "resourceId": "123456",
            "apiId": "1234567890",
            "stage": "dev",
            "protocol": "http",
            "requestTime": "",
            "requestTimeEpoch": datetime.now().timestamp(),
            "path": "/",
            "resourcePath": "/{proxy+}",
            "httpMethod": "GET",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
        },
        "queryStringParameters": query_parameters,
        "headers": {
            "Via": "1.1 0459f0f7053eeb224fd9fe0f5db5970a.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "*/*",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "vyulyh77tl.execute-api.us-east-2.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "v_y6ozGpbDIE-f8q8W5vo9Y1vIfc0ekaW2f-SbuDk7BCgac71k4m7Q==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": path_parameters,
        "httpMethod": "GET",
        "isBase64Encoded": False,
        "multiValueHeaders": {},
        "test": True,
        "stageVariables": {},
        "path": "/",
    }


def create_policy_role_data(identity_pool_id: str, value_type: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Federated": "cognito-identity.amazonaws.com"},
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {"cognito-identity.amazonaws.com:aud": identity_pool_id},
                    "ForAnyValue:StringLike": {"cognito-identity.amazonaws.com:amr": value_type},
                },
            }
        ],
    }
