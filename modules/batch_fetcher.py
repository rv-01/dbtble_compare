def fetch_data_batchwise(conn, schema, table, columns, primary_keys, where_clause, batch_size, offset):
    """
    Fetches a batch of data from the given table using the provided connection.
    Returns a list of rows (as tuples) and the column names.
    """
    cur = conn.cursor()
    # Build SELECT statement
    col_str = ', '.join(columns)
    pk_order = ', '.join(primary_keys)
    sql = f"SELECT {col_str} FROM {schema}.{table}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += f" ORDER BY {pk_order} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
    cur.execute(sql)
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    cur.close()
    return rows, col_names 