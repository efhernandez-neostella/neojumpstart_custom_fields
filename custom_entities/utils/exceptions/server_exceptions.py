import json
from datetime import datetime


def error_handler(exc_type: str, error_msg: str, correlation_id: str) -> dict:

    return {
        'statusCode': 500,
        'body': json.dumps({
            'message': error_msg,
            'code': str(exc_type),
            'correlation_id': correlation_id
        }),
    }
