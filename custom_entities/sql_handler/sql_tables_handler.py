from core_utils.functions import (deserialize_rds_response,
                                  rds_execute_statement)
from utils.exceptions.controller_exceptions import ControllerException


class SQLTable:
    def __init__(self, native_query: str = "") -> None:
        self.query = native_query

    def select(self, table: str, select_query: list = []) -> "SQLTable":
        columns = select_query if len(select_query) >= 1 else "*"
        self.query += f"SELECT {', '.join(columns)} FROM {table} "
        return self

    def where(self, where_query: list = []) -> "SQLTable":
        if where_query:
            self.query += f"WHERE {' AND '.join(where_query)} "
        return self

    def limit(self, number_records: int = 1) -> "SQLTable":
        self.query += f"LIMIT {str(number_records)}"
        return self

    def count(self, table: str, count_query: list = []) -> "SQLTable":
        columns = count_query if len(count_query) >= 1 else "*"
        self.query += f"SELECT COUNT({', '.join(columns)}) FROM {table} "
        return self

    def insert(self, table: str, columns: list = []) -> "SQLTable":
        self.query += f"INSERT INTO {table}({', '.join(columns)}) "
        return self

    def values(self, values: list = [], tenant_id: str = "") -> "SQLTable":
        tenant_value_query = f", '{tenant_id}'" if len(tenant_id) > 1 else ""
        self.query += f"VALUES({', '.join(values)}{tenant_value_query}) "
        return self

    def on_conflict(self, on_conflict_column: str) -> "SQLTable":
        self.query += f"ON CONFLICT({on_conflict_column}) DO "
        return self

    def return_value(self, return_value: str) -> "SQLTable":
        self.query += f"RETURNING {return_value}"
        return self

    def update(self, table: str, update_values: list = []) -> "SQLTable":
        self.query += f"UPDATE {table} SET {', '.join(update_values)} "
        return self

    def order_by(self, order_by_query: str) -> "SQLTable":
        self.query += f"ORDER BY {order_by_query} "
        return self

    def group_by(self, values: list) -> "SQLTable":
        self.query += f"GROUP BY {', '.join(values)} "
        return self

    def inner_join(self, inner_join_query: str) -> "SQLTable":
        self.query += f"INNER JOIN {inner_join_query} "
        return self

    def left_join(self, left_join_query: str) -> "SQLTable":
        self.query += f"LEFT JOIN {left_join_query} "
        return self

    def right_join(self, right_join_query: str) -> "SQLTable":
        self.query += f"RIGHT JOIN {right_join_query} "
        return self

    def delete(self, delete_from: str) -> "SQLTable":
        self.query += f"DELETE FROM {delete_from} "
        return self

    def where_in(self, column: str, values: list) -> "SQLTable":
        if values:
            self.query += f"WHERE {column} IN ({', '.join(values)}) "
        return self


def execute(query: str, table: str = "", mandatory_result: bool = True) -> list:
    """Execute the given query, deserialize it and check if the record exists.

    Args:
        query (str): SQL query
        table (str, optional): table name of the query. Defaults to ''
        mandatory_result(bool, optional): whether the result to search is mandatory or not.

    Raises:
        ControllerException: Raise a not found error if the record don't exists

    Returns:
        list: the records for the given query.
    """
    records = deserialize_rds_response(rds_execute_statement(query))
    if len(records):
        return records
    if not mandatory_result:
        return []
    print("QUERY: ", query)
    raise ControllerException(
        404,
        {
            "code": "NoRecordsFound",
            "exception": f"No records found for table: {table}, with the given value",
        },
    )
