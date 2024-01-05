from controllers.tenants import TenantPut

from core_utils.functions import get_unit_test_user
from utils.mocks import event_object_mock

USER_ID = get_unit_test_user()


def test_create_tenant_resources():
    event = event_object_mock(USER_ID,
                              body={
                                  'tenant_id': '8b3dfcf7-3ac1-4090-a97b-4451def9f796',
                                  'admin_email': 'cdbzprogr@gmail.com',
                                  'admin_first_name': 'Carlos',
                                  'admin_last_name': 'Bolivar',
                                  'tenant_name': 'carlostenant',
                                  'is_active': True
                              })
    event.pop('test')
    response = TenantPut.lambda_handler(event, '')
    print(response)
    assert response['statusCode'] == 200
