import json
import os
import time
from uuid import uuid4

import boto3
from models.Tenant import TenantModel

from core_utils.functions import (
    COGNITO_CLIENT,
    RDS_CLIENT,
    REGION_NAME,
    SERVICE_NAME,
    UUID,
    DB_CLUSTER_ARN,
    DB_CREDENTIALS_SECRETS_STORE_ARN,
    DATABASE_NAME,
    confirm_transaction,
    create_transaction,
    delete_transaction,
    rds_execute_statement,
)
from sql_handler.sql_tables_handler import execute
from utils.common import format_nulleable_values
from utils.constants import AUTH_ROLE_POLICY, UNAUTH_ROLE_POLICY
from utils.exceptions.controller_exceptions import ControllerException
from utils.mocks import create_policy_role_data

STAGE = os.getenv("STAGE")
CUSTOM_MESSAGE_TRIGGER = os.getenv("CUSTOM_MESSAGE_TRIGGER")
POST_AUTHENTICATION_TRIGGER = os.getenv("POST_AUTHENTICATION_TRIGGER")
GATEWAY_AUTHORIZER_ID = os.getenv("GATEWAY_AUTHORIZER_ID")
REST_API_ID = os.getenv("REST_API_ID")
GATEWAY_CLIENT = boto3.client("apigateway")
SES_EMAIL = os.getenv("SES_EMAIL")
SES_EMAIL_ARN = os.getenv("SES_EMAIL_ARN")


class TenantCreationProcess:
    def __init__(self, tenant_object: "TenantModel"):
        self.tenant_object = tenant_object
        self.database_keys: dict = {
            "dbInstanceIdentifier": None,
            "engine": "aurora-postgresql",
            "host": None,
            "port": 5432,
            "resourceId": None,
            "username": None,
            "password": uuid4().hex,
        }
        self.database_name: str = None
        self.db_cluster_arn: str = None
        self.db_credentials_secrets_store_arn: str = None
        self.user_pool_id: str = None
        self.user_pool_arn: str = None
        self.user_pool_id_appkey = None
        self.user_pool_client_id: str = None
        self.identity_pool_id: str = None
        self.authorized_role_arn: str = None
        self.unauthorized_role_arn: str = None

    def valid_unique_fields(self) -> None:
        valid, code = self.tenant_object.tenant_name_exists()
        if not valid:
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "tenant_id": None,
                    "result": f"""The tenant with name {self.tenant_object.fields['tenant_name']}
                      already exist.""",
                    "code": code,
                },
            )
        valid, code = self.tenant_object.subdomain_exists()
        if not valid:
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "tenant_id": None,
                    "result": f"""The tenant with subdomain {self.tenant_object.fields['subdomain']}
                      already exist.""",
                    "code": code,
                },
            )

    def create_database_secrets(self) -> str:
        client = boto3.client("secretsmanager")
        response = client.create_secret(
            Name=f"{STAGE}/{self.database_keys['resourceId']}",
            Description="Tenant created from Custom Fields.",
            SecretString=json.dumps(self.database_keys),
        )
        db_credentials_secrets_store_arn = response["ARN"]
        return db_credentials_secrets_store_arn

    def get_rds_network_configuration(self):  # -> tuple[list, list]:
        client = boto3.client("ec2")
        vpc_id = client.describe_vpcs()["Vpcs"][0]["VpcId"]
        subnets = client.describe_subnets()["Subnets"]
        security_groups = client.describe_security_groups()["SecurityGroups"]
        vpc_security_groups = [
            security_group["GroupId"]
            for security_group in security_groups
            if security_group["VpcId"] == vpc_id
        ]
        availability_zones = [
            subnet["AvailabilityZone"] for subnet in subnets if subnet["VpcId"] == vpc_id
        ]

        return [vpc_security_groups[0]], availability_zones

    def create_database(self) -> None:
        self.database_name = f"{self.tenant_object.fields['subdomain']}{STAGE}masteruserdb"
        sql_query = f"CREATE DATABASE {self.database_name}"
        params = {
            "secretArn": DB_CREDENTIALS_SECRETS_STORE_ARN,
            "resourceArn": DB_CLUSTER_ARN,
            "database": DATABASE_NAME,
            "sql": sql_query,
            "includeResultMetadata": True,
        }
        RDS_CLIENT.execute_statement(**params)
        self.db_cluster_arn = DB_CLUSTER_ARN
        self.db_credentials_secrets_store_arn = DB_CREDENTIALS_SECRETS_STORE_ARN

    def create_tenant_database(self) -> None:
        self.database_name = f"{self.tenant_object.fields['subdomain']}{STAGE}MasterUserDB"
        self.database_keys["username"] = self.tenant_object.fields["subdomain"]
        self.database_keys[
            "dbInstanceIdentifier"
        ] = f"{self.tenant_object.fields['subdomain']}-{STAGE}-masterusercluster"
        self.database_keys[
            "resourceId"
        ] = f"{self.tenant_object.fields['subdomain']}-{STAGE}-masterusercluster"
        vpc_security_groups, availability_zones = self.get_rds_network_configuration()
        client = boto3.client("rds")
        response = client.create_db_cluster(
            AvailabilityZones=availability_zones,
            MasterUsername=self.database_keys["username"],
            MasterUserPassword=self.database_keys["password"],
            DatabaseName=self.database_name,
            DBClusterIdentifier=self.database_keys["dbInstanceIdentifier"],
            Engine=self.database_keys["engine"],
            Port=self.database_keys["port"],
            DeletionProtection=False,
            EnableHttpEndpoint=True,
            EngineMode="serverless",
            DBSubnetGroupName="default",
            VpcSecurityGroupIds=vpc_security_groups,
            ScalingConfiguration={
                "MinCapacity": 8,
                "MaxCapacity": 16,
                "AutoPause": True,
                "SecondsUntilAutoPause": 7200,
            },
        )
        self.database_keys["host"] = response["DBCluster"]["Endpoint"]
        self.db_cluster_arn = response["DBCluster"]["DBClusterArn"]
        self.db_credentials_secrets_store_arn = self.create_database_secrets()

    def check_db_availability(self) -> None:
        client = boto3.client("rds")
        status = "creating"
        wait_for_response = 10
        while status != "available":
            rds_instance = client.describe_db_clusters(
                DBClusterIdentifier=self.database_keys["dbInstanceIdentifier"]
            )
            status = rds_instance["DBClusters"][0]["Status"]
            time.sleep(wait_for_response)

    def run_migrations(self) -> None:
        all_migrations = TenantModel.get_migrations()
        transactionId = create_transaction(
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
        )

        try:
            for migration in all_migrations:
                if migration["file_content"] is not None:
                    rds_execute_statement(
                        sql=migration["file_content"],
                        database=self.database_name,
                        resourceArn=self.db_cluster_arn,
                        secretArn=self.db_credentials_secrets_store_arn,
                        transactionId=transactionId,
                    )

                    insert_migration_information = TenantModel.insert_migration_statement(
                        file_path=migration["file_path"],
                        file_content=migration["file_content"],
                    )

                    rds_execute_statement(
                        sql=insert_migration_information,
                        database=self.database_name,
                        resourceArn=self.db_cluster_arn,
                        secretArn=self.db_credentials_secrets_store_arn,
                        transactionId=transactionId,
                    )

            confirm_transaction(
                resourceArn=self.db_cluster_arn,
                secretArn=self.db_credentials_secrets_store_arn,
                transactionId=transactionId,
            )
        except Exception as e:
            delete_transaction(
                resourceArn=self.db_cluster_arn,
                secretArn=self.db_credentials_secrets_store_arn,
                transactionId=transactionId,
            )
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "tenant_id": self.tenant_object.fields["tenant_id"],
                    "result": f"Migration has errors {str(e)}.",
                    "code": "Tenant.MigrationErrors",
                },
            )

    def create_user_pool(self) -> None:
        subdomain = self.tenant_object.fields["subdomain"]
        # Main Userpool
        userpool = COGNITO_CLIENT.create_user_pool(
            PoolName=f"{STAGE}-MasterUserPool-{subdomain}",
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": False,
                    "TemporaryPasswordValidityDays": 7,
                }
            },
            EmailConfiguration={
                "EmailSendingAccount": "DEVELOPER",
                "From": f"{subdomain} <{SES_EMAIL}>",
                "SourceArn": SES_EMAIL_ARN,
                "ReplyToEmailAddress": SES_EMAIL,
            },
            LambdaConfig={
                "CustomMessage": CUSTOM_MESSAGE_TRIGGER,
                "PostAuthentication": POST_AUTHENTICATION_TRIGGER,
            },
            UsernameAttributes=["email"],
            Schema=[
                {
                    "Name": "email",
                    "AttributeDataType": "String",
                    "Mutable": True,
                    "Required": False,
                }
            ],
            AdminCreateUserConfig={
                "AllowAdminCreateUserOnly": True,
            },
            AccountRecoverySetting={
                "RecoveryMechanisms": [
                    {"Priority": 1, "Name": "verified_email"},
                ]
            },
        )
        self.user_pool_id = userpool["UserPool"]["Id"]
        self.user_pool_arn = userpool["UserPool"]["Arn"]

        userpool_client = COGNITO_CLIENT.create_user_pool_client(
            UserPoolId=self.user_pool_id,
            ClientName=f"{STAGE}-masteruserpoolclient-{subdomain}",
            GenerateSecret=False,
            RefreshTokenValidity=30,
            AccessTokenValidity=60,
            IdTokenValidity=60,
            TokenValidityUnits={
                "RefreshToken": "days",
                "AccessToken": "minutes",
                "IdToken": "minutes",
            },
            CallbackURLs=[
                "https://www.google.com",
            ],
            ReadAttributes=["email"],
            WriteAttributes=["email"],
            ExplicitAuthFlows=[
                "ALLOW_CUSTOM_AUTH",
                "ALLOW_USER_SRP_AUTH",
                "ALLOW_REFRESH_TOKEN_AUTH",
            ],
            SupportedIdentityProviders=["COGNITO"],
            AllowedOAuthFlows=["code", "implicit"],
            AllowedOAuthScopes=[
                "email",
                "openid",
                "aws.cognito.signin.user.admin",
                "profile",
            ],
            PreventUserExistenceErrors="ENABLED",
            EnableTokenRevocation=True,
        )

        self.user_pool_client_id = userpool_client["UserPoolClient"]["ClientId"]

        # Appkey Userpool
        userpool_appkey = COGNITO_CLIENT.create_user_pool(
            PoolName=f"{STAGE}-MasterUserPool-{subdomain}-appkey",
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": True,
                    "TemporaryPasswordValidityDays": 7,
                }
            },
            UsernameAttributes=["email"],
            Schema=[
                {
                    "Name": "email",
                    "AttributeDataType": "String",
                    "Mutable": True,
                    "Required": False,
                }
            ],
            AdminCreateUserConfig={
                "AllowAdminCreateUserOnly": False,
            },
        )
        self.user_pool_id_appkey = userpool_appkey["UserPool"]["Id"]

        COGNITO_CLIENT.create_user_pool_client(
            UserPoolId=userpool_appkey["UserPool"]["Id"],
            ClientName=f"{STAGE}-masteruserpoolclient-{subdomain}-appkey",
            GenerateSecret=True,
            RefreshTokenValidity=30,
            AccessTokenValidity=60,
            IdTokenValidity=60,
            TokenValidityUnits={
                "RefreshToken": "days",
                "AccessToken": "minutes",
                "IdToken": "minutes",
            },
            ExplicitAuthFlows=[
                "ALLOW_CUSTOM_AUTH",
                "ALLOW_USER_SRP_AUTH",
                "ALLOW_REFRESH_TOKEN_AUTH",
            ],
            SupportedIdentityProviders=["COGNITO"],
            AllowedOAuthFlows=["client_credentials"],
            PreventUserExistenceErrors="ENABLED",
            EnableTokenRevocation=True,
        )

    def create_userpool_domain(self) -> None:
        COGNITO_CLIENT.create_user_pool_domain(
            Domain=f"{SERVICE_NAME}-{self.tenant_object.fields['subdomain']}",
            UserPoolId=self.user_pool_id_appkey,
        )

    def create_identity_pool(self) -> None:
        client = boto3.client("cognito-identity")
        subdomain = self.tenant_object.fields["subdomain"]

        response = client.create_identity_pool(
            IdentityPoolName=f"{STAGE}-masteridentitypool-{subdomain}",
            AllowClassicFlow=False,
            AllowUnauthenticatedIdentities=False,
            CognitoIdentityProviders=[
                {
                    "ProviderName": f"cognito-idp.{REGION_NAME}.amazonaws.com/{self.user_pool_id}",
                    "ClientId": self.user_pool_client_id,
                    "ServerSideTokenCheck": False,
                }
            ],
        )
        self.identity_pool_id = response["IdentityPoolId"]

    def create_tenant_cognito_policies_and_roles(self) -> None:
        client = boto3.client("iam")
        authorized_policy = client.create_policy(
            PolicyName=f"{STAGE}-cognitoauthpolicy-{self.tenant_object.fields['subdomain']}",
            PolicyDocument=json.dumps(AUTH_ROLE_POLICY),
        )
        authorized_role = client.create_role(
            RoleName=f"{STAGE}-cognitoauthrole-{self.tenant_object.fields['subdomain']}",
            AssumeRolePolicyDocument=json.dumps(
                create_policy_role_data(self.identity_pool_id, "authenticated")
            ),
        )
        client.attach_role_policy(
            PolicyArn=authorized_policy["Policy"]["Arn"],
            RoleName=authorized_role["Role"]["RoleName"],
        )
        unauthorized_policy = client.create_policy(
            PolicyName=f"{STAGE}-cognitounauthpolicy-{self.tenant_object.fields['subdomain']}",
            PolicyDocument=json.dumps(UNAUTH_ROLE_POLICY),
        )
        unauthorized_role = client.create_role(
            RoleName=f"{SERVICE_NAME}-cognitounauthrole-{self.tenant_object.fields['subdomain']}",
            AssumeRolePolicyDocument=json.dumps(
                create_policy_role_data(self.identity_pool_id, "unauthenticated")
            ),
        )
        client.attach_role_policy(
            PolicyArn=unauthorized_policy["Policy"]["Arn"],
            RoleName=unauthorized_role["Role"]["RoleName"],
        )

        self.authorized_role_arn = authorized_role["Role"]["Arn"]
        self.unauthorized_role_arn = unauthorized_role["Role"]["Arn"]

    def attach_roles_to_identity_pool(self) -> None:
        client = boto3.client("cognito-identity")
        client.set_identity_pool_roles(
            IdentityPoolId=self.identity_pool_id,
            Roles={
                "authenticated": self.authorized_role_arn,
                "unauthenticated": self.unauthorized_role_arn,
            },
        )

    def create_tenant_translation_bucket(self) -> None:
        bucket_name = os.environ.get("TRANSLATIONS_BUCKET")

        translation_content = {"en": {}, "es": {}}
        body = json.dumps(translation_content, indent=2, default=str)
        s3_resource = boto3.resource("s3")
        s3_bucket = s3_resource.Bucket(name=bucket_name)

        s3_bucket.put_object(Key=f"{self.tenant_object.fields['tenant_id']}.json", Body=body)

    def create_user(self) -> str:
        try:
            cognito_response = COGNITO_CLIENT.admin_get_user(
                UserPoolId=self.user_pool_id,
                Username=self.tenant_object.fields["admin_email"],
            )
            cognito_user_id = cognito_response["Username"]

        except COGNITO_CLIENT.exceptions.UserNotFoundException:
            cognito_response = COGNITO_CLIENT.admin_create_user(
                UserPoolId=self.user_pool_id,
                Username=self.tenant_object.fields["admin_email"],
                UserAttributes=[
                    {"Name": "email_verified", "Value": "True"},
                    {
                        "Name": "email",
                        "Value": self.tenant_object.fields["admin_email"],
                    },
                    {
                        "Name": "name",
                        "Value": self.tenant_object.fields["admin_first_name"],
                    },
                ],
                DesiredDeliveryMediums=["EMAIL"],
            )
            cognito_user_id = cognito_response["User"]["Username"]
        return cognito_user_id

    def deploy_api_gateway(self):
        GATEWAY_CLIENT.create_deployment(restApiId=REST_API_ID, stageName=STAGE)

    def update_gateaway_authorizer(self) -> None:
        GATEWAY_CLIENT.update_authorizer(
            restApiId=REST_API_ID,
            authorizerId=GATEWAY_AUTHORIZER_ID,
            patchOperations=[
                {
                    "op": "add",
                    "path": "/providerARNs",
                    "value": self.user_pool_arn,
                },
            ],
        )

    def add_tenant_record(self) -> None:
        self.tenant_object.fields["database_name"] = self.database_name
        self.tenant_object.fields["db_cluster_arn"] = self.db_cluster_arn
        self.tenant_object.fields[
            "db_credentials_secrets_store_arn"
        ] = self.db_credentials_secrets_store_arn
        self.tenant_object.fields["user_pool_id"] = self.user_pool_id
        self.tenant_object.fields["identity_pool_id"] = self.identity_pool_id
        self.tenant_object.fields["user_pool_client_id"] = self.user_pool_client_id
        # Create the record on master tenant and the same record for new tenant
        self.tenant_object.update_new_tenant_record()
        new_tenant_sql = self.tenant_object.new_tenant_insert_query()
        rds_execute_statement(
            sql=new_tenant_sql,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
        )

    def create_init_objects(self, objects: list, transactionId: str) -> None:
        str_instert = ""
        for _object in objects:
            str_instert += (
                f"('{_object['object_id']}', '{self.tenant_object.fields['tenant_id']}', "
                f"'{_object['table_name']}', {_object['object_limit']}, "
                f"'{_object['friendly_name_column']}', "
                f"{format_nulleable_values(_object['primary_object'])}, "
                f"{format_nulleable_values(_object['linking_table'])},"
                f"{format_nulleable_values(_object['parent_object'])},"
                f"{format_nulleable_values(_object['component_id'])}) ,"
            )

        rds_execute_statement(
            sql=TenantModel.insert_objects_query(str_instert[:-1]),
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )

    def create_init_entities(self, entities: list, transactionId: str) -> None:
        str_instert = ""
        for _entity in entities:
            str_instert += (
                f"('{_entity['entity_id']}',  "
                f"'{_entity['database_name']}', {_entity['entity_limit']}, "
                f"'{_entity['translation_reference']}', "
                f"{format_nulleable_values(_entity['linking_table'])},"
                f"{format_nulleable_values(_entity['name'])},"
                f"{format_nulleable_values(_entity['api_name'])}) ,"
            )

        rds_execute_statement(
            sql=TenantModel.insert_entities_query(str_instert[:-1]),
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )

    def create_init_components(self, components: list, transactionId: str) -> None:
        str_instert = ""
        for _component in components:
            str_instert += (
                f"('{_component['components_id']}', {_component['is_active']}, "
                f"'{_component['module']}', '{_component['component']}',  "
                f"'{_component['subcomponent']}', '{_component['valid_for']}') ,"
            )
        insert_sql = TenantModel.insert_components_query(str_instert[:-1])
        rds_execute_statement(
            sql=insert_sql,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )

    def create_user_record(self, transactionId: str, cognito_user_id: str):
        create_user_query = self.tenant_object.create_user(cognito_user_id=cognito_user_id)
        # Create user for the new tenant
        rds_execute_statement(
            sql=create_user_query,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )
        # Create user for the old tenant
        rds_execute_statement(sql=create_user_query)

    def create_role(self, transactionId: str, cognito_user_id: str, role_name: str):
        create_role_query = self.tenant_object.create_role(role_name=role_name)

        role_id = rds_execute_statement(
            sql=create_role_query,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )["records"][0][0]["stringValue"]

        create_user_role_relation_query = self.tenant_object.create_user_role(
            cognito_user_id=cognito_user_id, role_id=role_id
        )
        rds_execute_statement(
            sql=create_user_role_relation_query,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )

    def create_custom_user(self, transactionId: str, first_name: str, last_name: str, email: str):
        appkey_user_query = self.tenant_object.create_custom_user(
            first_name=first_name, last_name=last_name, email=email
        )
        rds_execute_statement(
            sql=appkey_user_query,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
            transactionId=transactionId,
        )

    def create_records_process(
        self, entities: list, objects: list, components: list, cognito_user_id: str
    ):
        transactionId = create_transaction(
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            secretArn=self.db_credentials_secrets_store_arn,
        )
        try:
            self.create_user_record(transactionId=transactionId, cognito_user_id=cognito_user_id)
            self.create_custom_user(
                transactionId=transactionId,
                first_name="App",
                last_name="Key",
                email="App-Key",
            )
            self.create_custom_user(
                transactionId=transactionId,
                first_name="Tenant",
                last_name="Key",
                email="Tenant-Key",
            )
            self.create_role(
                transactionId=transactionId,
                cognito_user_id=cognito_user_id,
                role_name="default",
            )
            self.create_role(
                transactionId=transactionId,
                cognito_user_id=cognito_user_id,
                role_name="admin",
            )
            self.create_role(
                transactionId=transactionId,
                cognito_user_id=cognito_user_id,
                role_name="account",
            )
            self.create_init_components(components=components, transactionId=transactionId)
            self.create_init_objects(objects=objects, transactionId=transactionId)

            confirm_transaction(
                resourceArn=self.db_cluster_arn,
                secretArn=self.db_credentials_secrets_store_arn,
                transactionId=transactionId,
            )
        except Exception as e:
            print(str(e))
            delete_transaction(
                resourceArn=self.db_cluster_arn,
                secretArn=self.db_credentials_secrets_store_arn,
                transactionId=transactionId,
            )
            raise ControllerException(
                400,
                {
                    "correlation_id": UUID,
                    "tenant_id": None,
                    "result": f"Error during tenant data creation. {str(e)}",
                    "code": "Tenant.TenantDataCreation",
                },
            )

    def add_cname_resource_record(self):
        domain_name = os.environ.get("DOMAIN_NAME")
        subdomain = self.tenant_object.fields["subdomain"]
        HOSTED_ZONE_ID = os.getenv("HOSTED_ZONE_ID")
        client = boto3.client("route53")
        DNS = f"{subdomain}.{STAGE}.{domain_name}"
        URL = f"{STAGE}.{domain_name}"

        client.change_resource_record_sets(
            HostedZoneId=HOSTED_ZONE_ID,
            ChangeBatch={
                "Comment": f"create cname record for tenant {subdomain}",
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": DNS,
                            "ResourceRecords": [{"Value": URL}],
                            "TTL": 60,
                            "Type": "CNAME",
                        },
                    }
                ],
            },
        )

        return DNS

    def create_tenant(self):
        # Database creation
        tenant_url = self.add_cname_resource_record()
        self.create_database()
        self.run_migrations()
        self.create_user_pool()
        self.create_userpool_domain()
        self.create_identity_pool()
        self.create_tenant_cognito_policies_and_roles()
        self.attach_roles_to_identity_pool()
        self.update_gateaway_authorizer()
        self.deploy_api_gateway()
        self.create_tenant_translation_bucket()
        self.add_tenant_record()
        cognito_user_id = self.create_user()
        components = self.tenant_object.get_components()
        objects = self.tenant_object.get_objects()
        entities = self.tenant_object.get_entities()
        self.create_records_process(entities, objects, components, cognito_user_id)

        return (
            200,
            {
                "correlation_id": UUID,
                "tenant_id": self.tenant_object.fields["tenant_id"],
                "result": "Create tenant success",
                "cognito_user_id": cognito_user_id,
                "tenant_url": tenant_url,
            },
        )
