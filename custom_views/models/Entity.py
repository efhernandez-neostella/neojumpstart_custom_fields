from sql_handler.sql_tables_handler import SQLTable, execute


class EntityModel:
    def __init__(self, entity_id: str):
        self.table: str = "entities_master"
        self.entity_id: str = entity_id

    def get_entity_by_id(self) -> dict:
        sql = SQLTable()
        table = f"{self.table} e"
        select_query = ["e.database_name", "e.api_name"]
        where_query = [f"e.entity_id = '{self.entity_id}'"]

        query = sql.select(table, select_query).where(where_query)
        rds_entity = execute(query.query, table, False)
        primary_key = self.get_table_primary_key(rds_entity[0].get("database_name"))
        rds_entity[0].update(primary_key)
        return rds_entity[0]

    def get_table_primary_key(self, database_name: str):
        select_query = ["c.column_name AS database_entity_primary_key"]
        table = "information_schema.key_column_usage AS c"
        left_join_query = "information_schema.table_constraints AS \
            t ON t.constraint_name = c.constraint_name"
        where_query = [
            f"t.table_name = '{database_name}'",
            "t.constraint_type = 'PRIMARY KEY'",
        ]
        query = (
            SQLTable()
            .select(table, select_query)
            .left_join(left_join_query)
            .where(where_query)
            .query
        )
        return execute(query, table, False)[0]
