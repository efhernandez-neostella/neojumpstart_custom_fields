import json

from controllers.tenants import TenantGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


def test_get_success_tenant():
    event = event_object_mock(USER_ID, path_parameters={
                              'tenant_id': '3a57aa9b-ac5b-4f60-9c5e-b7300f2bac26'})
    response = TenantGet.lambda_handler(event, '')
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert 'correlation_id' in body
    assert 'tenant' in body
    assert 'tenant_id' in body['tenant']
    assert 'tenant_name' in body['tenant']
    assert 'is_active' in body['tenant']
    assert 'admin_name' in body['tenant']
    assert 'subdomain' in body['tenant']
    assert 'activation_date' in body['tenant']
    assert 'admin_email' in body['tenant']
    assert 'status' in body['tenant']
    assert 'created_by' in body['tenant']
    assert 'updated_by' in body['tenant']
    assert 'created_at' in body['tenant']
    assert 'updated_at' in body['tenant']


def test_get_empty_tenant():
    event = event_object_mock(USER_ID, path_parameters={
                              'tenant_id': '3a57aa9b-ac5b-4f60-9c5e-b7300f2bac27'})
    response = TenantGet.lambda_handler(event, '')
    assert response['statusCode'] == 400


def test_get_bad_params_tenant():
    event = event_object_mock(USER_ID, path_parameters={
                              'id': '3a57aa9b-ac5b-4f60-9c5e-b7300f2bac27'})
    response = TenantGet.lambda_handler(event, '')
    assert response['statusCode'] == 400
