import json

from controllers.tenants import TenantsGet

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


def test_get_success_tenants():
    event = event_object_mock(USER_ID)
    response = TenantsGet.lambda_handler(event, '')
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert 'tenants' in body
    assert 'correlation_id' in body
    for tenant in body['tenants']:
        assert 'tenant_id' in tenant
        assert 'tenant_name' in tenant
        assert 'is_active' in tenant
        assert 'admin_name' in tenant
        assert 'subdomain' in tenant
        assert 'activation_date' in tenant
        assert 'admin_email' in tenant
        assert 'status' in tenant
        assert 'created_by' in tenant
        assert 'updated_by' in tenant
        assert 'created_at' in tenant
        assert 'updated_at' in tenant


def test_get_active_tenants():
    event = event_object_mock(USER_ID, query_parameters={
                              'is_active': 'true'})
    response = TenantsGet.lambda_handler(event, '')
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert 'tenants' in body
    assert 'correlation_id' in body
    for tenant in body['tenants']:
        assert 'tenant_id' in tenant
        assert 'tenant_name' in tenant
        assert 'is_active' in tenant
        assert True == tenant['is_active']
        assert 'admin_name' in tenant
        assert 'subdomain' in tenant
        assert 'activation_date' in tenant
        assert 'admin_email' in tenant
        assert 'status' in tenant
        assert 'created_by' in tenant
        assert 'updated_by' in tenant
        assert 'created_at' in tenant
        assert 'updated_at' in tenant


def test_get_inactive_tenants():
    event = event_object_mock(USER_ID, query_parameters={
                              'is_active': 'false'})
    response = TenantsGet.lambda_handler(event, '')
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert 'tenants' in body
    assert 'correlation_id' in body
    for tenant in body['tenants']:
        assert 'tenant_id' in tenant
        assert 'tenant_name' in tenant
        assert 'is_active' in tenant
        assert False == tenant['is_active']
        assert 'admin_name' in tenant
        assert 'subdomain' in tenant
        assert 'activation_date' in tenant
        assert 'admin_email' in tenant
        assert 'status' in tenant
        assert 'created_by' in tenant
        assert 'updated_by' in tenant
        assert 'created_at' in tenant
        assert 'updated_at' in tenant


def test_get_bad_params_tenant():
    event = event_object_mock(USER_ID, query_parameters={
                              'is_active': 'error'})
    response = TenantsGet.lambda_handler(event, '')
    assert response['statusCode'] == 400
