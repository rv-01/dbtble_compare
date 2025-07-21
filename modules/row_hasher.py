import hashlib

def hash_rows(rows, col_names, exclude_columns=None):
    """
    Hashes each row (excluding specified columns) using SHA256.
    Returns a dict: {primary_key_tuple: row_hash}
    """
    if exclude_columns is None:
        exclude_columns = []
    col_indices = [i for i, col in enumerate(col_names) if col not in exclude_columns]
    pk_indices = [i for i, col in enumerate(col_names) if col in exclude_columns or False]  # Placeholder, actual PK indices should be passed if needed
    row_hashes = {}
    for row in rows:
        # Only hash non-excluded columns
        values = [str(row[i]) if row[i] is not None else '' for i in col_indices]
        row_str = '|'.join(values)
        row_hash = hashlib.sha256(row_str.encode('utf-8')).hexdigest()
        # Use all columns as PK if PK indices not provided (should be improved)
        pk = tuple(row[i] for i in pk_indices) if pk_indices else tuple(row[i] for i in range(len(row)))
        row_hashes[pk] = row_hash
    return row_hashes 