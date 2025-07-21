import logging
import getpass
import uuid
import time

def log_to_audit_table(conn, audit_table, event_data):
    """
    Logs comparison events and mismatches to an audit table in the DB if enabled.
    event_data: dict with keys like job_id, user_name, event_time, event_type, table, schema, batch_id, row_counts, mismatch_count, status, error_message, details.
    """
    cur = conn.cursor()
    columns = ', '.join(event_data.keys())
    values = ', '.join([f":{k}" for k in event_data.keys()])
    sql = f"INSERT INTO {audit_table} ({columns}) VALUES ({values})"
    cur.execute(sql, event_data)
    conn.commit()
    cur.close()
    logging.info(f"Logged to audit table: {event_data}")

def log_batch_event(conn, audit_table, job_id, table, schema, batch_id, row_counts, mismatch_count, status, details=None):
    event_data = {
        'job_id': job_id,
        'user_name': getpass.getuser(),
        'event_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        'event_type': 'BATCH',
        'table_name': table,
        'schema_name': schema,
        'batch_id': batch_id,
        'row_counts': row_counts,
        'mismatch_count': mismatch_count,
        'status': status,
        'error_message': None,
        'details': details
    }
    log_to_audit_table(conn, audit_table, event_data)

def log_error_event(conn, audit_table, job_id, table, schema, batch_id, error_message, details=None):
    event_data = {
        'job_id': job_id,
        'user_name': getpass.getuser(),
        'event_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        'event_type': 'ERROR',
        'table_name': table,
        'schema_name': schema,
        'batch_id': batch_id,
        'row_counts': None,
        'mismatch_count': None,
        'status': 'ERROR',
        'error_message': error_message,
        'details': details
    }
    log_to_audit_table(conn, audit_table, event_data)

def log_event(message, level='info'):
    """
    Logs an event to the audit log file and stdout.
    """
    if level == 'debug':
        logging.debug(message)
    else:
        logging.info(message) 