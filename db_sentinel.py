"""
DB_Sentinel_util_super: Main entrypoint for robust Oracle table comparison and sync utility.
"""
import os
import sys
import logging
import uuid
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from modules.config_loader import load_config
from modules.db_connector import OracleDBConnector
from modules.batch_fetcher import fetch_data_batchwise
from modules.row_hasher import hash_rows
from modules.comparator import compare_hashes
from modules.sql_generator import generate_sql_file
from modules.audit_logger import log_to_audit_table, log_event, log_batch_event, log_error_event
from modules.checkpoint_manager import save_batch_checkpoint, load_batch_checkpoint
from modules.reverifier import verify_primary_keys
import time
import csv

def setup_logging(audit_log_path, debug=False):
    os.makedirs(os.path.dirname(audit_log_path), exist_ok=True)
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        filename=audit_log_path,
        level=level,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def process_table(table_cfg, config, source_db, target_db, job_id, run_id, ui_progress_hook=None):
    schema = table_cfg['schema']
    table = table_cfg['table_name']
    primary_keys = table_cfg['primary_key']
    batch_size = table_cfg.get('chunk_size', 1000)
    columns = table_cfg.get('columns')
    where_clause = table_cfg.get('where_clause')
    exclude_columns = table_cfg.get('exclude_columns', [])  # Per-table exclude_columns
    audit_table = config['paths'].get('audit_table', 'DB_SENTINEL_AUDIT')
    metadata_table = config['paths'].get('metadata_table', 'DB_SENTINEL_METADATA')
    enable_audit = config['flags'].get('enable_audit_table', False)
    enable_restart = config['flags'].get('enable_restart', False)
    enable_reverification = config['flags'].get('enable_reverification', False)
    debug = config.get('flags', {}).get('debug', False)

    # Timestamped per-table SQL output files
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)
    source_sql_path = os.path.join(output_dir, f'source_{table}_sync_{run_id}.sql')
    target_sql_path = os.path.join(output_dir, f'target_{table}_sync_{run_id}.sql')

    try:
        # 1. Get total row count for progress
        with source_db.get_cursor() as cur:
            count_sql = f"SELECT COUNT(1) FROM {schema}.{table}"
            if where_clause:
                count_sql += f" WHERE {where_clause}"
            if debug:
                log_event(f"Count SQL: {count_sql}", level='debug')
            cur.execute(count_sql)
            total_rows = cur.fetchone()[0]

        # 2. Determine columns to use
        if not columns:
            with source_db.get_cursor() as cur:
                cur.execute(f"SELECT * FROM {schema}.{table} WHERE 1=0")
                columns = [desc[0] for desc in cur.description]
        if debug:
            log_event(f"Columns for {schema}.{table}: {columns}", level='debug')

        # 3. Prepare for restart/resume
        offset = 0
        batch_id_start = 0
        if enable_restart:
            # Find the last completed batch for this job/table
            last_batch = 0
            for i in range((total_rows + batch_size - 1) // batch_size):
                checkpoint = load_batch_checkpoint(source_db.conn, metadata_table, job_id, table, schema, i)
                if checkpoint and checkpoint.get('status') == 'COMPLETED':
                    last_batch = i + 1
            offset = last_batch * batch_size
            batch_id_start = last_batch
            if last_batch > 0:
                log_event(f"Resuming {schema}.{table} from batch {last_batch} (offset {offset})")

        # 4. Batch processing with threading and tqdm
        max_threads = config.get('max_threads', 4)  # Configurable number of threads
        n_batches = (total_rows - offset + batch_size - 1) // batch_size
        mismatches = []
        missing_in_source = []
        missing_in_target = []
        source_rows = {}
        target_rows = {}
        start_time = time.time()
        log_event(f"Using multithreading with max_threads={max_threads} for batch execution.")
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {}
            for i in range(n_batches):
                batch_offset = offset + i * batch_size
                batch_id = batch_id_start + i
                if debug:
                    log_event(f"Submitting batch {batch_id} (offset={batch_offset}, size={batch_size}) for {schema}.{table}", level='debug')
                futures[executor.submit(
                    process_batch, source_db, target_db, schema, table, columns, primary_keys, where_clause, batch_size, batch_offset, exclude_columns, source_rows, target_rows, batch_id
                )] = batch_id
            for f in tqdm(as_completed(futures), total=n_batches, desc=f"Comparing {schema}.{table}"):
                batch_id = futures[f]
                try:
                    batch_result = f.result()
                    if debug:
                        log_event(f"Batch {batch_id} result: {batch_result}", level='debug')
                    mismatches.extend(batch_result['mismatches'])
                    missing_in_source.extend(batch_result['missing_in_source'])
                    missing_in_target.extend(batch_result['missing_in_target'])
                    # Save checkpoint after each batch
                    if enable_restart:
                        save_batch_checkpoint(source_db.conn, metadata_table, {
                            'job_id': job_id,
                            'table_name': table,
                            'schema_name': schema,
                            'batch_id': batch_id,
                            'last_offset': batch_result['offset'] + batch_size,
                            'processed_rows': batch_result['processed_rows'],
                            'total_rows': total_rows,
                            'status': 'COMPLETED',
                            'error_message': None,
                            'last_processed_time': time.strftime('%Y-%m-%d %H:%M:%S')
                        })
                    if enable_audit:
                        log_batch_event(source_db.conn, audit_table, job_id, table, schema, batch_id, batch_result['processed_rows'], len(batch_result['mismatches']), 'COMPLETED')
                    if ui_progress_hook:
                        ui_progress_hook(table, batch_id, n_batches)
                except Exception as e:
                    import traceback
                    log_event(f"Exception in batch {batch_id}: {e}\n{traceback.format_exc()}", level='debug')
                    if enable_restart:
                        save_batch_checkpoint(source_db.conn, metadata_table, {
                            'job_id': job_id,
                            'table_name': table,
                            'schema_name': schema,
                            'batch_id': batch_id,
                            'last_offset': batch_offset,
                            'processed_rows': 0,
                            'total_rows': total_rows,
                            'status': 'ERROR',
                            'error_message': str(e),
                            'last_processed_time': time.strftime('%Y-%m-%d %H:%M:%S')
                        })
                    if enable_audit:
                        log_error_event(source_db.conn, audit_table, job_id, table, schema, batch_id, str(e))
                    log_event(f"Error in batch {batch_id} of {schema}.{table}: {e}")
        end_time = time.time()

        # Debug: show sample PKs and counts after comparison
        if debug:
            log_event(f"Total mismatches: {len(mismatches)}, sample: {list(mismatches)[:5]}", level='debug')
            log_event(f"Total missing_in_source: {len(missing_in_source)}, sample: {list(missing_in_source)[:5]}", level='debug')
            log_event(f"Total missing_in_target: {len(missing_in_target)}, sample: {list(missing_in_target)[:5]}", level='debug')

        # 7. Reverification step
        safe_to_insert = set(missing_in_target)
        valid_update_pks = set(mismatches)
        no_op_update_pks = set()
        if enable_reverification:
            # Debug log PKs to verify for INSERT
            log_event(f"PKs to verify for INSERT: {missing_in_target}", level='debug')
            safe_to_insert = verify_primary_keys(target_db.conn, f"{schema}.{table}", primary_keys, missing_in_target, max_threads=max_threads)
            log_event(f"PKs safe to insert after verification: {safe_to_insert}", level='debug')
            # Debug log PKs to verify for UPDATE
            log_event(f"PKs to verify for UPDATE: {mismatches}", level='debug')
            valid_update_pks = verify_primary_keys(target_db.conn, f"{schema}.{table}", primary_keys, mismatches, max_threads=max_threads)
            log_event(f"PKs valid for update after verification: {valid_update_pks}", level='debug')
            no_op_update_pks = set(mismatches) - set(valid_update_pks)
            if no_op_update_pks:
                log_event(f"No-op UPDATE PKs (not present in target): {no_op_update_pks}", level='debug')
                log_event(f"No-op UPDATE count: {len(no_op_update_pks)}", level='debug')

        # 8. Final report
        log_event(f"Table {schema}.{table} compared. Mismatches: {len(mismatches)}, Missing in source: {len(missing_in_source)}, Missing in target: {len(missing_in_target)}")

        # 5. Generate SQL files (per-table, per-run) with verified PKs
        if debug:
            log_event(f"Generating SQL for {schema}.{table}: {len(valid_update_pks)} UPDATEs, {len(safe_to_insert)} INSERTs", level='debug')
        generate_sql_file(
            valid_update_pks,  # Only verified PKs for UPDATE
            missing_in_source,
            safe_to_insert,    # Only safe PKs for INSERT
            columns,
            source_rows,
            target_rows,
            primary_keys,
            source_sql_path,
            target_sql_path,
            table_name=f"{schema}.{table}"
        )
    except Exception as e:
        import traceback
        log_event(f"Exception in process_table for {schema}.{table}: {e}\n{traceback.format_exc()}", level='debug')
        raise

    # Return summary for comparison report
    return {
        'job_id': job_id,
        'table_name': table,
        'schema': schema,
        'row_counts': total_rows,
        'mismatch_count': len(mismatches),
        'missing_in_source': len(missing_in_source),
        'missing_in_target': len(missing_in_target),
        'status': 'COMPLETED' if not mismatches else 'MISMATCH',
        'start_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
        'end_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)),
        'source_sql_file': os.path.basename(source_sql_path),
        'target_sql_file': os.path.basename(target_sql_path),
        'no_op_update_count': len(no_op_update_pks),
    }


def process_batch(source_db, target_db, schema, table, columns, primary_keys, where_clause, batch_size, offset, exclude_columns, source_rows, target_rows, batch_id):
    # Fetch batch from source
    src_rows, col_names = fetch_data_batchwise(source_db.conn, schema, table, columns, primary_keys, where_clause, batch_size, offset)
    # Fetch batch from target
    tgt_rows, _ = fetch_data_batchwise(target_db.conn, schema, table, columns, primary_keys, where_clause, batch_size, offset)
    # Debug: log number of rows fetched
    log_event(f"Batch {batch_id} fetched {len(src_rows)} source rows, {len(tgt_rows)} target rows for {schema}.{table}", level='debug')
    # Hash rows
    src_hashes = hash_rows(src_rows, col_names, exclude_columns)
    tgt_hashes = hash_rows(tgt_rows, col_names, exclude_columns)
    # Debug: log sample hashes
    log_event(f"Batch {batch_id} sample source hashes: {list(src_hashes.items())[:3]}", level='debug')
    log_event(f"Batch {batch_id} sample target hashes: {list(tgt_hashes.items())[:3]}", level='debug')
    # Store rows for SQL gen
    for row in src_rows:
        pk = tuple(row[col_names.index(pk)] for pk in primary_keys)
        source_rows[pk] = row
    for row in tgt_rows:
        pk = tuple(row[col_names.index(pk)] for pk in primary_keys)
        target_rows[pk] = row
    # Compare
    mismatches, missing_in_source, missing_in_target = compare_hashes(src_hashes, tgt_hashes)
    # Debug: log comparison result
    log_event(f"Batch {batch_id} comparison: {len(mismatches)} mismatches, {len(missing_in_source)} missing in source, {len(missing_in_target)} missing in target", level='debug')
    return {
        'mismatches': mismatches,
        'missing_in_source': missing_in_source,
        'missing_in_target': missing_in_target,
        'offset': offset,
        'processed_rows': len(src_rows)
    }


def main(ui_progress_hook=None):
    # 1. Load config
    config = load_config('config.yaml')
    debug = config.get('flags', {}).get('debug', False)
    if debug:
        from pprint import pformat
        log_event(f"Loaded config: {pformat(config)}", level='debug')

    # 2. Setup logging
    setup_logging(config['paths']['audit_log'], debug=debug)
    log_event("DB_Sentinel_util_super started.")

    # 3. Generate a unique job_id and run_id for this run
    job_id = str(uuid.uuid4())
    run_id = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    # 4. Connect to source and target DBs
    comparison_results = []
    with OracleDBConnector(config['source_db']) as source_db, OracleDBConnector(config['target_db']) as target_db:
        # 5. For each table in config, orchestrate comparison
        for table_cfg in config['table_config']:
            result = process_table(table_cfg, config, source_db, target_db, job_id, run_id, ui_progress_hook)
            comparison_results.append(result)

    # 6. Write comparison report as CSV (timestamped)
    report_path = f"./output/comparison_report_{run_id}.csv"
    if comparison_results:
        with open(report_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=comparison_results[0].keys())
            writer.writeheader()
            writer.writerows(comparison_results)
        log_event(f"Comparison report written to {report_path}")
    else:
        log_event("No comparison results to write to report.")

    log_event(f"DB_Sentinel_util_super completed. Job ID: {job_id}")

if __name__ == "__main__":
    main() 