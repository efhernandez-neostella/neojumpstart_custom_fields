from models.Tenant import TenantModel

from core_utils.functions import COGNITO_CLIENT, UUID


class TenantsPut:
    def __init__(self, tenant_object: "TenantModel", user_id: str):
        self.tenant_object: "TenantModel" = tenant_object
        self.user_pool_id = None
        self.cognito_user_id = None
        self.user_id = user_id

    def update_tenant_user_cognito(self):
        COGNITO_CLIENT.admin_update_user_attributes(
            UserPoolId=self.user_pool_id,
            Username=self.cognito_user_id,
            UserAttributes=[{"Name": "email", "Value": self.tenant_object.fields["admin_email"]}],
        )

    def perform(self):
        self.tenant_object.tenant_exist()
        self.tenant_object.update_tenant_name_available()
        self.tenant_object.tenant_available()
        self.user_pool_id, self.cognito_user_id = self.tenant_object.get_tenant_user_pool()
        self.update_tenant_user_cognito()
        self.tenant_object.update_tenant_data(
            use_main_tenant=True, cognito_user_id=self.cognito_user_id, user_id=f"'{self.user_id}'"
        )
        self.tenant_object.update_tenant_data(
            use_main_tenant=False, cognito_user_id=self.cognito_user_id
        )

        return (
            200,
            {
                "correlation_id": UUID,
                "tenant_id": self.tenant_object.fields["tenant_id"],
                "result": "Tenant update success",
            },
        )
