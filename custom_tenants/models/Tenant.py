import re

from core_utils.functions import (
    UUID,
    confirm_transaction,
    create_transaction,
    delete_transaction,
    deserialize_rds_response,
    rds_execute_statement,
)
from sql_handler.sql_tables_handler import SQLTable
from sql_handler.sql_views_handler import SQLView
from utils.exceptions.controller_exceptions import ControllerException
from utils.common import format_for_rds

EMAIL_RE = (
    r"([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")"
    r"@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])"
)


class TenantModel:
    def __init__(
        self, created_by: str = None, updated_by: str = None, tenant_id: str = None
    ) -> None:
        self.table: str = "tenants_master"
        self.created_by = created_by
        self.updated_by = updated_by
        self.tenant_id = tenant_id
        self.fields: dict = {
            "tenant_id": None,
            "is_active": True,
            "user_pool_id": None,
            "identity_pool_id": None,
            "user_pool_client_id": None,
            "database_name": None,
            "db_cluster_arn": None,
            "db_credentials_secrets_store_arn": None,
            "admin_email": None,
            "admin_first_name": None,
            "admin_last_name": None,
            "tenant_name": None,
            "subdomain": None,
            "date": None,
        }

    def validate_payload(self, payload: dict = None):  # -> tuple[bool, str]:
        valid_fields: list = self.fields.keys()

        for key in payload.keys():
            if key not in valid_fields:
                return False, "Invalid Request Body"
            else:
                self.fields[key] = payload[key]  # Populate self.fields object

        if (
            self.fields["subdomain"] is None
            or len(self.fields["subdomain"]) == 0
            or not isinstance(self.fields["subdomain"], str)
        ):
            return False, "Tenant.SubdomainIsRequired"
        elif not re.match("^[a-z0-9]{3,20}$", self.fields["subdomain"]):
            return False, "Tenant.InvalidSubdomainFormat"

        if (
            self.fields["admin_email"] is None
            or len(self.fields["admin_email"]) == 0
            or not isinstance(self.fields["admin_email"], str)
        ):
            return False, "Tenant.EmailIsRequired"
        elif not re.match(EMAIL_RE, self.fields["admin_email"]):
            return False, "Tenant.InvalidEmail"

        if (
            self.fields["admin_first_name"] is None
            or len(self.fields["admin_first_name"]) == 0
            or not isinstance(self.fields["admin_first_name"], str)
        ):
            return False, "Tenant.AdminFirstNameIsRequired"

        if (
            self.fields["admin_last_name"] is None
            or len(self.fields["admin_last_name"]) == 0
            or not isinstance(self.fields["admin_last_name"], str)
        ):
            return False, "Tenant.AdminLastNameIsRequired"

        if (
            self.fields["tenant_name"] is None
            or len(self.fields["tenant_name"]) == 0
            or not isinstance(self.fields["tenant_name"], str)
        ):
            return False, "Tenant.TenantNameIsRequired"

        if self.fields["is_active"] is None or not isinstance(self.fields["is_active"], bool):
            return False, "Tenant.IsActiveIsRequired"

        return True, ""

    def tenants_get_validate_payload(self, payload: dict = None):
        valid_fields: list = self.fields.keys()

        for key in payload.keys():
            if key not in valid_fields:
                return False, "Invalid Request Body"
            else:
                self.fields[key] = payload[key]  # Populate self.fields object

        if isinstance(self.fields["is_active"], str):
            if self.fields["is_active"] not in ["true", "false"]:
                return False, "Tenant.IsActiveInvalidValue"

        return True, ""

    def tenant_get_validate_payload(self, payload: dict = None):
        valid_fields: list = self.fields.keys()

        for key in payload.keys():
            if key not in valid_fields:
                return False, "Invalid Request Body"
            else:
                self.fields[key] = payload[key]  # Populate self.fields object

        if (
            self.fields["tenant_id"] is None
            or len(self.fields["tenant_id"]) == 0
            or not isinstance(self.fields["tenant_id"], str)
        ):
            return False, "Tenant.TenantIdIsRequired"

        return True, ""

    # -> tuple[bool, str]:
    def tenant_put_validate_payload(self, payload: dict = None):
        valid_fields: list = self.fields.keys()

        for key in payload.keys():
            if key not in valid_fields:
                return False, "Invalid Request Body"
            else:
                self.fields[key] = payload[key]  # Populate self.fields object

        if (
            self.fields["tenant_id"] is None
            or len(self.fields["tenant_id"]) == 0
            or not isinstance(self.fields["tenant_id"], str)
        ):
            return False, "Tenant.TenantIdIsRequired"

        if (
            self.fields["admin_email"] is None
            or len(self.fields["admin_email"]) == 0
            or not isinstance(self.fields["admin_email"], str)
        ):
            return False, "Tenant.EmailIsRequired"
        elif not re.match(EMAIL_RE, self.fields["admin_email"]):
            return False, "Tenant.InvalidEmail"

        if (
            self.fields["admin_first_name"] is None
            or len(self.fields["admin_first_name"]) == 0
            or not isinstance(self.fields["admin_first_name"], str)
        ):
            return False, "Tenant.AdminFirstNameIsRequired"

        if (
            self.fields["admin_last_name"] is None
            or len(self.fields["admin_last_name"]) == 0
            or not isinstance(self.fields["admin_last_name"], str)
        ):
            return False, "Tenant.AdminLastNameIsRequired"

        if (
            self.fields["tenant_name"] is None
            or len(self.fields["tenant_name"]) == 0
            or not isinstance(self.fields["tenant_name"], str)
        ):
            return False, "Tenant.TenantNameIsRequired"

        if self.fields["is_active"] is None or not isinstance(self.fields["is_active"], bool):
            return False, "Tenant.IsActiveIsRequired"

        return True, ""

    def create_tenant_record(self, user_id):
        sql = (
            SQLTable()
            .insert(
                self.table,
                [
                    "subdomain",
                    "admin_email",
                    "admin_first_name",
                    "admin_last_name",
                    "tenant_name",
                    "is_active",
                    "status",
                    "user_pool_id",
                    "identity_pool_id",
                    "user_pool_client_id",
                    "created_by",
                    "updated_by",
                    "is_master_tenant",
                    "activation_date",
                ],
            )
            .values(
                [
                    f"{format_for_rds(self.fields['subdomain'])}",
                    f"{format_for_rds(self.fields['admin_email'])}",
                    f"{format_for_rds(self.fields['admin_first_name'])}",
                    f"{format_for_rds(self.fields['admin_last_name'])}",
                    f"{format_for_rds(self.fields['tenant_name'])}",
                    "TRUE",
                    "'creating'",
                    "''",
                    "''",
                    "''",
                    f"'{user_id}'",
                    f"'{user_id}'",
                    "FALSE",
                    "NOW()::date",
                ]
            )
            .return_value("tenant_id")
            .query
        )
        tenant_id = deserialize_rds_response(rds_execute_statement(sql))[0]["tenant_id"]
        return tenant_id

    def tenant_name_exists(self):  # -> tuple[bool, str]:
        sql = (
            SQLTable()
            .select(self.table, ["COUNT(*)"])
            .where([f"tenant_name ILIKE {format_for_rds(self.fields['tenant_name'])}"])
            .query
        )
        result = rds_execute_statement(sql)["records"][0][0]["longValue"]
        if result > 0:
            return False, "Tenant.TenantNameAlreadyExists"
        return True, ""

    def subdomain_exists(self):  # -> tuple[bool, str]:
        sql = (
            SQLTable()
            .select(self.table, ["COUNT(*)"])
            .where([f"subdomain LIKE {format_for_rds(self.fields['subdomain'])}"])
            .query
        )
        result = rds_execute_statement(sql)["records"][0][0]["longValue"]
        if result > 0:
            return False, "Tenant.SubdomainAlreadyExists"
        return True, ""

    @staticmethod
    def get_migrations() -> list:
        sql = (
            SQLView()
            .select("file_path, file_content FROM migrations_master")
            .order_by("created_at")
            .query
        )
        result = deserialize_rds_response(rds_execute_statement(sql))
        return result

    @staticmethod
    def insert_migration_statement(file_path: str, file_content: str) -> str:
        file_content = file_content.replace("'", r"\'")
        sql = (
            SQLTable()
            .insert("migrations_master", ["file_path", "file_content"])
            .values([f"'{file_path}'", f"e'{file_content}'"])
            .query
        )
        return sql

    def new_tenant_insert_query(self, use_user: bool = False) -> str:
        user_id = "NULL"
        if use_user:
            user_id = f"'{self.created_by}'"
        sql = (
            SQLTable()
            .insert(
                self.table,
                [
                    "tenant_id",
                    "is_active",
                    "user_pool_id",
                    "identity_pool_id",
                    "user_pool_client_id",
                    "tenant_name",
                    "is_master_tenant",
                    "database_name",
                    "db_cluster_arn",
                    "db_credentials_secrets_store_arn",
                    "admin_email",
                    "subdomain",
                    "admin_first_name",
                    "admin_last_name",
                    "activation_date",
                    "created_by",
                ],
            )
            .values(
                [
                    f"{format_for_rds(self.fields['tenant_id'])}",
                    "TRUE",
                    f"{format_for_rds(self.fields['user_pool_id'])}",
                    f"{format_for_rds(self.fields['identity_pool_id'])}",
                    f"{format_for_rds(self.fields['user_pool_client_id'])}",
                    f"{format_for_rds(self.fields['tenant_name'])}",
                    "false",
                    f"{format_for_rds(self.fields['database_name'])}",
                    f"{format_for_rds(self.fields['db_cluster_arn'])}",
                    f"{format_for_rds(self.fields['db_credentials_secrets_store_arn'])}",
                    f"{format_for_rds(self.fields['admin_email'])}",
                    f"{format_for_rds(self.fields['subdomain'])}",
                    f"{format_for_rds(self.fields['admin_first_name'])}",
                    f"{format_for_rds(self.fields['admin_last_name'])}",
                    "NOW()::date",
                    f"{user_id}",
                ]
            )
            .query
        )
        return sql

    def update_new_tenant_record(self) -> None:
        sql = (
            SQLTable()
            .update(
                self.table,
                [
                    "status = 'available'",
                    f"user_pool_id = '{self.fields['user_pool_id']}'",
                    f"identity_pool_id = '{self.fields['identity_pool_id']}'",
                    f"user_pool_client_id = '{self.fields['user_pool_client_id']}'",
                    f"database_name = {format_for_rds(self.fields['database_name'])}",
                    f"db_cluster_arn = {format_for_rds(self.fields['db_cluster_arn'])}",
                    f"""db_credentials_secrets_store_arn =
                    '{self.fields['db_credentials_secrets_store_arn']}'""",
                ],
            )
            .where([f"tenant_id = '{self.fields['tenant_id']}'"])
            .query
        )
        rds_execute_statement(sql)

    def get_objects(self) -> list:
        """
        Get all current objects from tenant master
        """
        sql = (
            SQLView()
            .select(
                """o.object_id, o.table_name, o.object_limit, o.friendly_name_column,
                o.primary_object, o.linking_table, o.parent_object, o.component_id
                  FROM objects_master o"""
            )
            .where(f"o.tenant_id = '{self.tenant_id}' AND o.table_name != 'tenants_master'")
            .order_by("o.parent_object DESC")
            .query
        )

        objects = deserialize_rds_response(rds_execute_statement(sql=sql))

        return objects

    @staticmethod
    def insert_objects_query(values):
        sql = SQLTable(
            f"""INSERT INTO objects_master (object_id,tenant_id, table_name, object_limit,
                    friendly_name_column, primary_object, linking_table, parent_object,
                    component_id) VALUES {values}"""
        ).query
        return sql

    def get_components(self) -> list:
        """
        Get all current components from tenants master
        """
        sql = (
            SQLView()
            .select(
                """components_id, is_active, module, component,
                subcomponent, valid_for FROM components_master where component != 'tenants'"""
            )
            .query
        )

        components = deserialize_rds_response(rds_execute_statement(sql=sql))

        return components

    @staticmethod
    def insert_components_query(values):
        sql = SQLTable(
            f"""INSERT INTO components_master (components_id, is_active, module, component,
                    subcomponent, valid_for) VALUES {values}"""
        ).query
        return sql

    def get_entities(self) -> list:
        """
        Get all current base entities from tenant master
        """
        sql = (
            SQLView()
            .select(
                """entity_id, database_name, entity_limit, translation_reference,
                    linking_table, name, api_name
                  FROM entities_master"""
            )
            .where("database_name != 'tenants_master'")
            .order_by("entity_id DESC")
            .query
        )

        entities = deserialize_rds_response(rds_execute_statement(sql=sql))

        return entities

    @staticmethod
    def insert_entities_query(values):
        sql = SQLTable(
            f"""INSERT INTO entities_master (entity_id, database_name,
                    entity_limit, translation_reference,
                    linking_table, name, api_name) VALUES {values}"""
        ).query
        return sql

    def create_user(self, cognito_user_id: str) -> str:
        sql = (
            SQLTable()
            .insert(
                "users_master", ["cognito_user_id", "first_name", "last_name", "email", "tenant_id"]
            )
            .values(
                [
                    f"'{cognito_user_id}'",
                    f"{format_for_rds(self.fields['admin_first_name'])}",
                    f"{format_for_rds(self.fields['admin_last_name'])}",
                    f"{format_for_rds(self.fields['admin_email'])}",
                    f"{format_for_rds(self.fields['tenant_id'])}",
                ]
            )
            .query
        )
        return sql

    def create_role(self, role_name: str) -> str:
        sql = (
            SQLTable()
            .insert("roles_master", ["role", "tenant_id", "type"])
            .values(
                [
                    f"'{role_name}'",
                    f"'{self.fields['tenant_id']}'",
                    f"{format_for_rds(role_name)}",
                ]
            )
            .return_value("role_id")
            .query
        )
        return sql

    def create_user_role(self, cognito_user_id: str, role_id: str):
        sql = (
            SQLTable()
            .insert("user_roles", ["tenant_id", "cognito_user_id", "role_id"])
            .values([f"'{self.fields['tenant_id']}'", f"'{cognito_user_id}'", f"'{role_id}'"])
            .query
        )
        return sql

    def create_custom_user(self, first_name, last_name, email) -> str:
        sql = (
            SQLTable()
            .insert(
                "users_master", ["cognito_user_id", "first_name", "last_name", "email", "tenant_id"]
            )
            .values(
                [
                    "uuid_generate_v4()",
                    f"{format_for_rds(first_name)}",
                    f"{format_for_rds(last_name)}",
                    f"{format_for_rds(email)}",
                    f"'{self.fields['tenant_id']}'",
                ]
            )
            .query
        )
        return sql

    def tenant_get_object_query(self, time_zone):
        sql_object = (
            SQLView()
            .select(
                f"""
            t.tenant_id, t.tenant_name, t.is_active, t.admin_first_name, t.admin_last_name,
            t.subdomain, to_char(t.activation_date,'MM/DD/YYYY') as activation_date,
            t.admin_email, t.status, u.full_name as created_by,
            t.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}' AS created_at,
            u2.full_name as updated_by,
            t.updated_at AT TIME ZONE 'UTC' AT TIME ZONE '{time_zone}' AS updated_at
             FROM {self.table} t"""
            )
            .left_join("users_master u ON t.created_by = u.cognito_user_id")
            .left_join("users_master u2 ON t.updated_by = u2.cognito_user_id")
        )

        return sql_object

    def tenant_get(self, time_zone):
        tenant_sql = self.tenant_get_object_query(time_zone)
        sql = tenant_sql.where(f"t.tenant_id = '{self.fields['tenant_id']}'").query

        tenant = deserialize_rds_response(rds_execute_statement(sql))[0]
        return tenant

    def tenants_get(self, time_zone):
        tenant_sql = self.tenant_get_object_query(time_zone)
        is_active_where = ""
        if isinstance(self.fields["is_active"], str) and self.fields["is_active"] == "true":
            is_active_where = f" AND t.is_active = {self.fields['is_active']}"
        sql = (
            tenant_sql.where("t.is_master_tenant = FALSE" + is_active_where)
            .order_by("t.created_at ASC")
            .query
        )
        tenants = deserialize_rds_response(rds_execute_statement(sql))
        return tenants

    def tenant_exist(self):
        valid, code = True, ""
        sql = (
            SQLTable()
            .select(self.table, ["COUNT(*)"])
            .where([f"tenant_id = '{self.fields['tenant_id']}'"])
            .query
        )
        result = rds_execute_statement(sql)["records"][0][0]["longValue"]
        if result == 0:
            valid, code = False, "Tenant.TenantDoesNotExist"

        if not valid:
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "result": f"The tenant with id {self.fields['tenant_id']} doesn't exist.",
                    "code": code,
                },
            )

    def update_tenant_name_available(self):
        sql = (
            SQLTable()
            .select(self.table, ["COUNT(*)"])
            .where(
                [
                    f"tenant_id != '{self.fields['tenant_id']}'",
                    f"tenant_name = {format_for_rds(self.fields['tenant_name'])}",
                ]
            )
            .query
        )
        result = rds_execute_statement(sql)["records"][0][0]["longValue"]

        if result > 0:
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "result": f"The tenant with name {self.fields['tenant_name']} already exists.",
                    "code": "Tenant.TenantNameAlreadyExist",
                },
            )

    def tenant_available(self):
        sql = (
            SQLTable()
            .select(self.table, ["COUNT(*)"])
            .where([f"tenant_id = '{self.fields['tenant_id']}'", "status = 'creating'"])
            .query
        )
        result = rds_execute_statement(sql)["records"][0][0]["longValue"]

        if result > 0:
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "result": f"The tenant {self.fields['tenant_name']} is on creating status.",
                    "code": "Tenant.CreationTenantStatus",
                },
            )

    def get_tenant_user_pool(self):
        sql = (
            SQLView()
            .select(
                f"""t.user_pool_id, u.cognito_user_id, database_name, db_cluster_arn,
                db_credentials_secrets_store_arn FROM {self.table} t"""
            )
            .inner_join("users_master u ON u.tenant_id = t.tenant_id")
            .where(f"u.email = t.admin_email AND t.tenant_id = '{self.fields['tenant_id']}'")
            .query
        )
        result = deserialize_rds_response(rds_execute_statement(sql))[0]
        user_pool_id, cognito_user_id = result["user_pool_id"], result["cognito_user_id"]
        self.fields["user_pool_id"] = user_pool_id
        self.fields["database_name"] = result["database_name"]
        self.fields["db_cluster_arn"] = result["db_cluster_arn"]
        self.fields["db_credentials_secrets_store_arn"] = result["db_credentials_secrets_store_arn"]
        return user_pool_id, cognito_user_id

    def update_tenant_data(
        self, use_main_tenant: bool, cognito_user_id: str, user_id: str = "NULL"
    ):
        tenant_sql = (
            SQLTable()
            .update(
                self.table,
                [
                    f"tenant_name={format_for_rds(self.fields['tenant_name'])}",
                    f"admin_email={format_for_rds(self.fields['admin_email'])}",
                    f"admin_first_name={format_for_rds(self.fields['admin_first_name'])}",
                    f"admin_last_name={format_for_rds(self.fields['admin_last_name'])}",
                    f"updated_by = {user_id}",
                    "updated_at = NOW()",
                ],
            )
            .where([f"tenant_id = '{self.fields['tenant_id']}'"])
            .query
        )
        user_sql = (
            SQLTable()
            .update(
                "users_master",
                [
                    f"email={format_for_rds(self.fields['admin_email'])}",
                    f"first_name={format_for_rds(self.fields['admin_first_name'])}",
                    f"last_name={format_for_rds(self.fields['admin_last_name'])}",
                    f"updated_by = {user_id}",
                    "updated_at = NOW()",
                ],
            )
            .where([f"cognito_user_id = '{cognito_user_id}'"])
            .query
        )
        if not use_main_tenant:
            try:
                transaction_id = create_transaction(
                    database=self.fields["database_name"],
                    resourceArn=self.fields["db_cluster_arn"],
                    secretArn=self.fields["db_credentials_secrets_store_arn"],
                )
                rds_params = {
                    "resourceArn": self.fields["db_cluster_arn"],
                    "secretArn": self.fields["db_credentials_secrets_store_arn"],
                    "transactionId": transaction_id,
                }
                rds_execute_statement(
                    sql=tenant_sql, database=self.fields["database_name"], **rds_params
                )
                rds_execute_statement(
                    sql=user_sql, database=self.fields["database_name"], **rds_params
                )
                confirm_transaction(**rds_params)
            except Exception as e:
                delete_transaction(**rds_params)
                raise ControllerException(400, {"result": f"Error during tenant update {e}"})
        else:
            rds_execute_statement(tenant_sql)
            rds_execute_statement(user_sql)
