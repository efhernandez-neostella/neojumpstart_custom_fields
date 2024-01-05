import json
import sys
from datetime import datetime

from models.Tenant import TenantModel
from process.tenants.TenantCreation import TenantCreationProcess

from core_utils.functions import (
    CORALOGIX_KEY,
    CURRENT_DATETIME,
    RESOURCE_METHOD,
    SERVICE_NAME,
    UUID,
    send_to_coralogix,
    wait_for_threads,
)
from utils.common import handle_payload_error
from utils.constants import HEADERS
from utils.exceptions.controller_exceptions import ControllerException
from utils.exceptions.server_exceptions import error_handler


# @webhook_dispatch('tenants_master', 'create') # TODO: add tenants_master to configuration.json file
def lambda_handler(event, _):
    try:
        send_to_coralogix(
            CORALOGIX_KEY,
            {"correlation_id": UUID, "Event Received": event},
            SERVICE_NAME,
            RESOURCE_METHOD,
            3,
        )

        user_id, tenant_id = event["user_id"], event["tenant_id"]
        payload = json.loads(event["body"])

        tenant_object = TenantModel(created_by=user_id, tenant_id=tenant_id)
        tenant_post_process = TenantCreationProcess(tenant_object)

        valid, code = tenant_object.validate_payload(payload)
        handle_payload_error(valid, code)

        response = tenant_post_process.create_tenant()

        return {"statusCode": response[0], "body": json.dumps(response[1]), "headers": HEADERS}
    except ControllerException as controller_exception:
        exception_response = controller_exception.response
        exception_response["headers"] = HEADERS
        return exception_response
    except Exception as error:
        exc_type, _, exc_tb = sys.exc_info()
        ERROR_MSG = f"Execution failed: {repr(error)}. Line: {str(exc_tb.tb_lineno)}."
        EXECUTION_TIME = str(datetime.now() - CURRENT_DATETIME)
        # Send error message and status to coralogix
        send_to_coralogix(
            CORALOGIX_KEY,
            {
                "correlation_id": UUID,
                "Status": "Failure",
                "Execution time": EXECUTION_TIME,
                "Error message": ERROR_MSG,
            },
            SERVICE_NAME,
            RESOURCE_METHOD,
            5,
        )
        wait_for_threads()
        error_response = error_handler(exc_type, ERROR_MSG, UUID)
        error_response["headers"] = HEADERS
        return error_response
