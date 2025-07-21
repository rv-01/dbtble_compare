from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def verify_primary_keys(conn, table, primary_keys, pk_values_list, max_threads=8):
    """
    Checks if PK rows already exist in the target DB using multithreading.
    Returns a set of PKs that do NOT exist (safe to insert or update).
    Shows progress with tqdm.
    """
    safe_to_insert = set()

    def check_pk(pk_values):
        where_clause = ' AND '.join([f"{col} = :{col}" for col in primary_keys])
        sql = f"SELECT COUNT(1) FROM {table} WHERE {where_clause}"
        params = {col: val for col, val in zip(primary_keys, pk_values)}
        cur = conn.cursor()
        cur.execute(sql, params)
        count = cur.fetchone()[0]
        cur.close()
        return pk_values if count == 0 else None

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(check_pk, pk): pk for pk in pk_values_list}
        for f in tqdm(as_completed(futures), total=len(pk_values_list), desc=f"Verifying PKs for {table}"):
            result = f.result()
            if result is not None:
                safe_to_insert.add(result)

    return safe_to_insert 