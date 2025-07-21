import streamlit as st
import threading
import time
import os
import yaml
import pandas as pd
from db_sentinel import main as run_db_sentinel
from modules.config_loader import load_config
from modules.db_connector import OracleDBConnector

def get_audit_records(conn, audit_table, limit=20):
    cur = conn.get_cursor()
    cur.execute(f"SELECT * FROM {audit_table} ORDER BY event_time DESC FETCH FIRST {limit} ROWS ONLY")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=col_names)

def get_metadata_records(conn, metadata_table, limit=20):
    cur = conn.get_cursor()
    cur.execute(f"SELECT * FROM {metadata_table} ORDER BY last_processed_time DESC FETCH FIRST {limit} ROWS ONLY")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=col_names)

st.set_page_config(page_title="DB Sentinel UI", layout="wide")
st.title("DB Sentinel - Oracle Table Comparison Utility")

# Load config.yaml
if not os.path.exists('config.yaml'):
    st.error("config.yaml not found in the current directory.")
    st.stop()

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Initialize session state for table configs
if 'table_config' not in st.session_state:
    st.session_state['table_config'] = config['table_config']

# Editable fields for source_db
st.header("Source DB")
config['source_db']['user'] = st.text_input("Source User", config['source_db']['user'])
config['source_db']['password'] = st.text_input("Source Password", config['source_db']['password'], type='password')
config['source_db']['dsn'] = st.text_input("Source DSN", config['source_db']['dsn'])

# Editable fields for target_db
st.header("Target DB")
config['target_db']['user'] = st.text_input("Target User", config['target_db']['user'])
config['target_db']['password'] = st.text_input("Target Password", config['target_db']['password'], type='password')
config['target_db']['dsn'] = st.text_input("Target DSN", config['target_db']['dsn'])

# Editable exclude columns
st.header("Exclude Columns")
exclude_columns = st.text_input("Exclude Columns (comma separated)", ','.join(config.get('exclude_columns', [])))
config['exclude_columns'] = [col.strip() for col in exclude_columns.split(',') if col.strip()]

# Editable paths
st.header("Paths")
config['paths']['audit_log'] = st.text_input("Audit Log Path", config['paths']['audit_log'])
config['paths']['source_sql_output'] = st.text_input("Source SQL Output Path", config['paths']['source_sql_output'])
config['paths']['target_sql_output'] = st.text_input("Target SQL Output Path", config['paths']['target_sql_output'])

# Editable flags
st.header("Flags")
config['flags']['enable_audit_table'] = st.checkbox("Enable Audit Table", config['flags'].get('enable_audit_table', True))
config['flags']['enable_reverification'] = st.checkbox("Enable Reverification", config['flags'].get('enable_reverification', True))
config['flags']['enable_restart'] = st.checkbox("Enable Restart", config['flags'].get('enable_restart', True))

# Editable table configs with Add/Delete functionality using session state
st.header("Table Configs")
tables_to_delete = []
for i, tbl in enumerate(st.session_state['table_config']):
    with st.expander(f"Table {i+1}: {tbl['table_name']}"):
        tbl['table_name'] = st.text_input(f"Table Name {i+1}", tbl['table_name'], key=f"table_name_{i}")
        tbl['schema'] = st.text_input(f"Schema {i+1}", tbl['schema'], key=f"schema_{i}")
        pk_str = st.text_input(f"Primary Keys {i+1} (comma separated)", ','.join(tbl['primary_key']), key=f"pk_{i}")
        tbl['primary_key'] = [k.strip() for k in pk_str.split(',') if k.strip()]
        tbl['chunk_size'] = st.number_input(f"Chunk Size {i+1}", value=tbl.get('chunk_size', 1000), min_value=1, key=f"chunk_{i}")
        columns_val = tbl.get('columns', [])
        if columns_val is None:
            columns_val = []
        columns_str = st.text_input(
            f"Columns {i+1} (comma separated, blank for all)",
            ','.join(columns_val),
            key=f"cols_{i}"
        )
        tbl['columns'] = [c.strip() for c in columns_str.split(',') if c.strip()] if columns_str else None
        where_clause = st.text_input(f"Where Clause {i+1}", tbl.get('where_clause', ''), key=f"where_{i}")
        tbl['where_clause'] = where_clause if where_clause else None
        if st.button(f"Delete Table {i+1}", key=f"delete_{i}"):
            tables_to_delete.append(i)
for idx in sorted(tables_to_delete, reverse=True):
    del st.session_state['table_config'][idx]
if st.button("Add Table"):
    st.session_state['table_config'].append({
        'table_name': '',
        'schema': '',
        'primary_key': [],
        'chunk_size': 1000,
        'columns': [],
        'where_clause': ''
    })

progress_placeholder = st.empty()
status_placeholder = st.empty()

# Shared state for progress
progress_state = {
    'table': None,
    'batch': 0,
    'total_batches': 1
}

def ui_progress_hook(table, batch_id, n_batches):
    progress_state['table'] = table
    progress_state['batch'] = batch_id + 1
    progress_state['total_batches'] = n_batches
    progress_placeholder.progress(batch_id / n_batches, text=f"{table}: Batch {batch_id+1}/{n_batches}")
    status_placeholder.info(f"Processing {table}: Batch {batch_id+1} of {n_batches}")

# Run job in a thread
def run_job_thread():
    config['table_config'] = st.session_state['table_config']
    with open('config.yaml', 'w') as f:
        yaml.safe_dump(config, f)
    run_db_sentinel(ui_progress_hook=ui_progress_hook)
    status_placeholder.success("Job completed!")

if st.button("Save & Start Job"):
    thread = threading.Thread(target=run_job_thread)
    thread.start()
    st.info("Job started. Progress will update below.")

# Show current progress
if progress_state['table']:
    progress_placeholder.progress(progress_state['batch'] / progress_state['total_batches'], text=f"{progress_state['table']}: Batch {progress_state['batch']}/{progress_state['total_batches']}")
    status_placeholder.info(f"Processing {progress_state['table']}: Batch {progress_state['batch']} of {progress_state['total_batches']}")

# Show recent audit/metadata records
with OracleDBConnector(config['source_db']) as conn:
    st.subheader("Recent Audit Records")
    st.dataframe(get_audit_records(conn, config['paths'].get('audit_table', 'DB_SENTINEL_AUDIT')))
    st.subheader("Recent Metadata Records")
    st.dataframe(get_metadata_records(conn, config['paths'].get('metadata_table', 'DB_SENTINEL_METADATA')))

# Download links for logs/output
st.subheader("Output Files")
if os.path.exists(config['paths']['audit_log']):
    st.download_button("Download Audit Log", open(config['paths']['audit_log'], "rb"), file_name="audit.log")
if os.path.exists(config['paths']['source_sql_output']):
    st.download_button("Download Source SQL", open(config['paths']['source_sql_output'], "rb"), file_name="source_sync_statements.sql")
if os.path.exists(config['paths']['target_sql_output']):
    st.download_button("Download Target SQL", open(config['paths']['target_sql_output'], "rb"), file_name="target_sync_statements.sql") 