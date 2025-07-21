def save_checkpoint(conn, metadata_table, checkpoint_data):
    """
    Saves checkpoint data to the metadata table.
    checkpoint_data: dict with keys like table, schema, batch_range, last_processed_time, etc.
    """
    cur = conn.cursor()
    columns = ', '.join(checkpoint_data.keys())
    values = ', '.join([f":{k}" for k in checkpoint_data.keys()])
    sql = f"MERGE INTO {metadata_table} USING dual ON (table_name = :table_name AND schema_name = :schema_name) \
            WHEN MATCHED THEN UPDATE SET {', '.join([f'{k} = :{k}' for k in checkpoint_data.keys() if k not in ['table_name', 'schema_name']])} \
            WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
    cur.execute(sql, checkpoint_data)
    conn.commit()
    cur.close()

def load_checkpoint(conn, metadata_table, table_name, schema_name):
    """
    Loads checkpoint data for a given table and schema from the metadata table.
    Returns a dict or None.
    """
    cur = conn.cursor()
    sql = f"SELECT * FROM {metadata_table} WHERE table_name = :table_name AND schema_name = :schema_name"
    cur.execute(sql, {'table_name': table_name, 'schema_name': schema_name})
    row = cur.fetchone()
    desc = [d[0] for d in cur.description]
    cur.close()
    if row:
        return dict(zip(desc, row))
    return None

def save_batch_checkpoint(conn, metadata_table, checkpoint_data):
    """
    Saves batch checkpoint data to the metadata table.
    checkpoint_data: dict with keys like job_id, table_name, schema_name, batch_id, last_offset, processed_rows, total_rows, status, error_message, last_processed_time.
    """
    cur = conn.cursor()
    columns = ', '.join(checkpoint_data.keys())
    values = ', '.join([f":{k}" for k in checkpoint_data.keys()])
    sql = f"MERGE INTO {metadata_table} USING dual ON (job_id = :job_id AND table_name = :table_name AND schema_name = :schema_name AND batch_id = :batch_id) \
            WHEN MATCHED THEN UPDATE SET {', '.join([f'{k} = :{k}' for k in checkpoint_data.keys() if k not in ['job_id', 'table_name', 'schema_name', 'batch_id']])} \
            WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
    cur.execute(sql, checkpoint_data)
    conn.commit()
    cur.close()

def load_batch_checkpoint(conn, metadata_table, job_id, table_name, schema_name, batch_id):
    """
    Loads batch checkpoint data for a given job, table, schema, and batch from the metadata table.
    Returns a dict or None.
    """
    cur = conn.cursor()
    sql = f"SELECT * FROM {metadata_table} WHERE job_id = :job_id AND table_name = :table_name AND schema_name = :schema_name AND batch_id = :batch_id"
    cur.execute(sql, {'job_id': job_id, 'table_name': table_name, 'schema_name': schema_name, 'batch_id': batch_id})
    row = cur.fetchone()
    desc = [d[0] for d in cur.description]
    cur.close()
    if row:
        return dict(zip(desc, row))
    return None 