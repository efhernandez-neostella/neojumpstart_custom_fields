

class SQLView:

    def __init__(self, native_query: str = "") -> None:
        self.query = native_query

    def select(self, select_query: str) -> 'SQLView':
        self.query += f"SELECT {select_query} "
        return self

    def inner_join(self, inner_join_query: str) -> 'SQLView':
        self.query += f"INNER JOIN {inner_join_query} "
        return self

    def left_join(self, left_join_query: str) -> 'SQLView':
        self.query += f"LEFT JOIN {left_join_query} "
        return self

    def right_join(self, right_join_query: str) -> 'SQLView':
        self.query += f"RIGHT JOIN {right_join_query} "
        return self

    def where(self, where_query) -> 'SQLView':
        self.query += f"WHERE {where_query} "
        return self

    def order_by(self, order_by_query) -> 'SQLView':
        self.query += f"ORDER BY {order_by_query} "
        return self
