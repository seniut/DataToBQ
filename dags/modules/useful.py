import os
from datetime import datetime, timezone
from time import time
from jinja2 import Template

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

DIRECTORY = os.path.dirname(os.path.abspath(__file__))

FOLDER_PATH = '{dir}/sql/{file_name}.sql'


def render_sql_from_file(file_name: str, params_dict: dict):
    # Read the SQL file
    with open(FOLDER_PATH.format(dir=DIRECTORY, file_name=file_name), 'r') as file:
        sql_content = file.read()

    # Render the SQL content
    rendered_sql = Template(sql_content).render(params_dict)
    print(f"SQL: {rendered_sql}")

    return rendered_sql


def run_sql_query(bigquery_client, table: str, params: dict):
    print(f"Params: {params}")
    query = render_sql_from_file(file_name=table, params_dict=params)
    job = bigquery_client.query(query)
    job.result()  # Wait for the job to complete


def build_statistics(
        project_id: str,
        dataset_id: str,
        table_name: str,
        bigquery_client,
        table_ref,
        start_time,
        dag_execution_date,
        etl_timestamp,
        statistics_table
):

    def count_distinct_rows():
        sql = f"SELECT COUNT(*) as distinct_count FROM (SELECT DISTINCT * FROM `{project_id}.{dataset_id}.{table_name}`)"
        query_job = bigquery_client.query(sql)
        results = query_job.result()
        return [row.distinct_count for row in results][0]

    print("...Gathering statistics...")

    load_to_bq_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    row_loaded_to_bq = bigquery_client.get_table(table_ref).num_rows
    distinct_row_count = count_distinct_rows()
    duration_loading_to_bq = round((time() - start_time) / 60, 2)
    rows_to_insert = [{
        "table_name": table_name,
        "dag_execution_date": dag_execution_date,
        "load_to_bq_timestamp": load_to_bq_timestamp,
        "etl_timestamp": etl_timestamp,
        "duration_loading_to_bq_min": duration_loading_to_bq,
        "row_loaded_to_bq": row_loaded_to_bq,
        "distinct_row_loaded_to_bq": distinct_row_count
    }]

    print(f"Statistics row: {rows_to_insert}")
    stats_table_ref = bigquery.TableReference(DatasetReference(project_id, dataset_id), statistics_table)
    errors = bigquery_client.insert_rows_json(stats_table_ref, rows_to_insert)
    if errors:
        print(f"Errors occurred while inserting rows: {errors}")


def update_etl_timestamp(bigquery_client, project_id, dataset_id, table_name, etl_timestamp):
    update_sql = f"""
                    ALTER TABLE `{project_id}.{dataset_id}.{table_name}` ADD COLUMN etl_timestamp TIMESTAMP;
                    
                    UPDATE `{project_id}.{dataset_id}.{table_name}` SET etl_timestamp = TIMESTAMP '{etl_timestamp}' WHERE TRUE;
                """
    print(f"SQL: {update_sql}")
    bigquery_client.query(update_sql).result()
