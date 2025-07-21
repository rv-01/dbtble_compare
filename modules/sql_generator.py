def sql_value(val):
    if val is None:
        return 'NULL'
    return "'{}'".format(str(val).replace("'", "''"))

def generate_sql_file(update_pks, missing_in_source, insert_pks, col_names, source_rows, target_rows, primary_keys, source_sql_path, target_sql_path, table_name=None):
    """
    Generates SQL INSERT/UPDATE statements for mismatches and missing rows.
    Writes to separate files for source and target.
    Only generates UPDATEs for PKs in update_pks, and INSERTs for PKs in insert_pks.
    table_name: required for correct SQL output
    """
    def row_to_dict(row):
        return dict(zip(col_names, row))

    if table_name is None:
        table_name = 'TABLE_NAME'

    with open(source_sql_path, 'a') as src_f, open(target_sql_path, 'a') as tgt_f:
        # INSERTs for rows missing in target (from source)
        for pk in insert_pks:
            row = source_rows[pk]
            d = row_to_dict(row)
            cols = ', '.join(d.keys())
            vals = ', '.join([sql_value(v) for v in d.values()])
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals});\n"
            src_f.write(sql)
        # INSERTs for rows missing in source (from target)
        for pk in missing_in_source:
            row = target_rows[pk]
            d = row_to_dict(row)
            cols = ', '.join(d.keys())
            vals = ', '.join([sql_value(v) for v in d.values()])
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals});\n"
            tgt_f.write(sql)
        # UPDATEs for mismatches (from source to target), only for PKs in update_pks
        for pk in update_pks:
            row = source_rows[pk]
            d = row_to_dict(row)
            set_clause = ', '.join([
                f"{col} = {sql_value(val)}" for col, val in d.items() if col not in primary_keys
            ])
            where_clause = ' AND '.join([
                f"{col} = {sql_value(d[col])}" for col in primary_keys
            ])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};\n"
            src_f.write(sql) 