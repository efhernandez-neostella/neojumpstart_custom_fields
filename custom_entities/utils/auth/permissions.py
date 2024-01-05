import json

from core_utils.functions import (
    check_tenant_level_permissions,
    check_user_level_permissions,
)
from utils.exceptions.controller_exceptions import ControllerException


class Permission:
    def __init__(
        self,
        user_id: str,
        tenant_id: str,
        event_uuid: str,
        module: str,
        component_name: str,
        subcomponent: str,
        action: str,
    ) -> None:
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.event_uuid = event_uuid
        self.module = module
        self.component_name = component_name
        self.subcomponent = subcomponent
        self.action = action

    def check_permissions(self) -> None:
        self._check_tenant_permissions()
        self._check_user_permissions()

    def _check_tenant_permissions(self) -> None:
        # Check tenant level permissions
        is_tenant_permitted = check_tenant_level_permissions(
            self.tenant_id, self.module, self.component_name, self.subcomponent
        )

        if not is_tenant_permitted:
            raise ControllerException(
                status=403,
                body={
                    "correlation_id": self.event_uuid,
                    "code": "role_permissions.TenantDoesNotHaveAccessToThisFeature",
                },
            )

    def _check_user_permissions(self) -> None:
        # Check user level permissions
        is_user_permitted = check_user_level_permissions(
            self.tenant_id,
            self.user_id,
            self.module,
            self.component_name,
            self.subcomponent,
            self.action,
        )

        if not is_user_permitted:
            raise ControllerException(
                status=403,
                body=json.dumps(
                    {
                        "correlation_id": self.event_uuid,
                        "code": "role_permissions.UserDoesNotHaveAccessToThisFeature",
                    }
                ),
            )
