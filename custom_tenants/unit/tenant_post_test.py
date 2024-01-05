from controllers.tenants import TenantPost

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


def test_create_tenant_resources():
    event = event_object_mock(USER_ID,
                              body={
                                  'subdomain': 'test18',
                                  'admin_email': 'carlos.bolivar@neostella.com',
                                  'admin_first_name': 'Test18',
                                  'admin_last_name': 'Test',
                                  'tenant_name': 'test18'
                              })
    event.pop('test')
    response = TenantPost.lambda_handler(event, '')
    assert response['statusCode'] == 200
