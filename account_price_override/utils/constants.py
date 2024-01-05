
HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS,POST,PUT,DELETE',
    'Access-Control-Allow-Headers': 'Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers'
}

AUTH_ROLE_POLICY = {
    'Version': '2012-10-17',
    'Statement': [
        {
            'Action': [
                'mobileanalytics:PutEvents',
                'cognito-sync:*',
                'cognito-identity:*'
            ],
            'Resource': [
                '*'
            ],
            'Effect': 'Allow'
        }
    ]
}

UNAUTH_ROLE_POLICY = {
    'Version': '2012-10-17',
    'Statement': [
        {
            'Action': [
                'mobileanalytics:PutEvents',
                'cognito-sync:*'
            ],
            'Resource': [
                '*'
            ],
            'Effect': 'Allow'
        }
    ]
}
