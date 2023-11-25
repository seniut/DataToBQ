from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from google.cloud import bigquery
from google.cloud import secretmanager_v1
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from google.cloud.bigquery import TableReference, DatasetReference
import io
import json
from modules import useful

from datetime import datetime, timezone, timedelta
from time import time

# DAG configuration
default_args = {
    'depends_on_past': False,
    'catchup': False,
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

execution_date = '{{ ts }}'

# GCP project ID
project_id = 'datatobq-405917'
# Name of the secret in Secret Manager
secret_id = 'csv-unloader-secret'
# ID of the Google Drive folder. Secret manager has to be added as Editor to shared folder
drive_folder_id = '1-4ElvxoHdIOl3uMjPILdme-YqXpO92Ep'

# Define the table schemas
schemas = {
    "app_ratings": [
        bigquery.SchemaField("package_name", "STRING"),
        bigquery.SchemaField("app_version_code", "FLOAT"),
        bigquery.SchemaField("app_version_name", "STRING"),
        bigquery.SchemaField("star_rating", "INTEGER"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("package", "STRING"),
    ],
    "config_history": [
        bigquery.SchemaField("package", "STRING"),
        bigquery.SchemaField("param_name", "STRING"),
        bigquery.SchemaField("version", "INTEGER"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("created_at", "STRING"),
        bigquery.SchemaField("published_at", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("config_values", "STRING"),
    ],
    "country_code_to_name": [
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("country_code_3", "STRING"),
    ],
    "country_names_to_code": [
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("country_code_3", "STRING"),
    ],
    "package_engagement_metrics": [
        bigquery.SchemaField("package", "STRING"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("version", "STRING"),
        bigquery.SchemaField("sessions", "FLOAT"),
        bigquery.SchemaField("avg_session_duration", "FLOAT"),
        bigquery.SchemaField("dau", "FLOAT"),
        bigquery.SchemaField("wau", "FLOAT"),
        bigquery.SchemaField("mau", "FLOAT"),
    ],
    "ua_campaign_spendings": [
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("costILS", "FLOAT"),
    ],
    "user_installs": [
        bigquery.SchemaField("package", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("firebase_partition", "STRING"),
        bigquery.SchemaField("mobile_brand_name", "STRING"),
        bigquery.SchemaField("mobile_marketing_name", "STRING"),
        bigquery.SchemaField("version", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("date", "STRING"),
    ],
    "user_subscription_trials": [
        bigquery.SchemaField("installTimestamp", "STRING"),
        bigquery.SchemaField("purchaseTimestamp", "STRING"),
        bigquery.SchemaField("packageName", "STRING"),
        bigquery.SchemaField("userID", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("orderID", "STRING"),
        bigquery.SchemaField("purchaseToken", "STRING"),
        bigquery.SchemaField("fcmToken", "FLOAT"),
        bigquery.SchemaField("version", "STRING"),
        bigquery.SchemaField("package", "STRING"),
    ],
    "statistics_drive_to_bq": [
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("dag_execution_date", "TIMESTAMP"),
        bigquery.SchemaField("load_to_bq_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("etl_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("duration_loading_to_bq_min", "FLOAT"),
        bigquery.SchemaField("row_loaded_to_bq", "INTEGER"),
        bigquery.SchemaField("distinct_row_loaded_to_bq", "INTEGER"),
    ],
    "analytics.statistics_analytics": [
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("dag_execution_date", "TIMESTAMP"),
        bigquery.SchemaField("load_to_bq_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("etl_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("duration_loading_to_bq_min", "FLOAT"),
        bigquery.SchemaField("row_loaded_to_bq", "INTEGER"),
        bigquery.SchemaField("distinct_row_loaded_to_bq", "INTEGER"),
    ],
}


##############################################################################################################

# DAG definition
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(1), tags=['etl'])
def create_and_load_tables():
    start = EmptyOperator(task_id='start')
    unit_load_files = EmptyOperator(task_id='unit_load_files')
    unit_analytics_mapping = EmptyOperator(task_id='unit_analytics_mapping')
    unit_analytics = EmptyOperator(task_id='unit_analytics')

    ########################################
    #   TASK 1: Loading from Drive to BQ
    ########################################
    @task
    def generate_etl_timestamp():
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    ########################################
    #   TASK 2: Creating table if not exists
    ########################################
    def create_table_task_factory(table_name, schema, dataset_id):
        @task(task_id=f"create_{table_name}_table")
        def create_table():
            table_id = f"{project_id}.{table_name}" if len(table_name.split('.')) == 2 else f"{project_id}.{dataset_id}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)

            # Drop the table if it exists
            if table_name not in ['statistics_drive_to_bq', 'analytics.statistics_analytics']:
                drop_sql = f"DROP TABLE IF EXISTS `{table.project}.{table.dataset_id}.{table.table_id}`;"
                bigquery_client.query(drop_sql).result()

            table = bigquery_client.create_table(table, exists_ok=True)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

        return create_table

    ########################################
    #   TASK 3: Loading from Drive to BQ
    ########################################
    def drive_to_bq_task_factory(table_name, schema_fields, dataset_id):
        @task(task_id=f"{table_name}_to_bq")
        def drive_to_bq(file_id, dag_execution_date, etl_timestamp):
            print(f"etl_timestamp: {etl_timestamp}")
            start_time = time()

            def download_file(file_id):
                request = drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                return fh

            file_obj = download_file(file_id)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header row
                autodetect=False,
                schema=schema_fields,
                # TODO: Maybe be made for delta loading: thought temp table and then DELETE by specific condition and INSERT
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Truncate the table before loading
            )

            dataset_ref = DatasetReference(project_id, dataset_id)
            table_ref = TableReference(dataset_ref, table_name)
            job = bigquery_client.load_table_from_file(file_obj, table_ref, job_config=job_config)
            job.result()

            # Update the etl_timestamp in the loaded data
            useful.update_etl_timestamp(bigquery_client, project_id, dataset_id, table_name, etl_timestamp)

            ########################################
            #   Gathering statistics
            ########################################
            # Calculate metrics
            useful.build_statistics(project_id, dataset_id, table_name, bigquery_client,
                                   table_ref,
                                   start_time,
                                   dag_execution_date,
                                   etl_timestamp,
                                   statistics_table='statistics_drive_to_bq')
            ########################################

        return drive_to_bq

    ########################################
    #   TASK 4: Creating Mapping Analytics table
    ########################################
    def analytics_mapping_task_factory(table_, dataset_id):
        @task(task_id=f"{table_}")
        def run_sql_query(table: str, params_: dict, dag_execution_date, etl_timestamp):
            start_time = time()
            useful.run_sql_query(bigquery_client, table, params_)

            ########################################
            #   Gathering statistics
            ########################################
            # Calculate metrics
            dataset_ref = DatasetReference(project_id, dataset_id)
            table_ref = TableReference(dataset_ref, table)
            useful.build_statistics(project_id, dataset_id, table, bigquery_client,
                                    table_ref,
                                    start_time,
                                    dag_execution_date,
                                    etl_timestamp,
                                    statistics_table='statistics_analytics')
            ########################################

        return run_sql_query

    ########################################
    #   TASK 5: Creating and Loading Analytics table
    ########################################
    def analytics_task_factory(table_, dataset_id):
        @task(task_id=f"{table_}")
        def run_sql_query(table: str, params_: dict, dag_execution_date, etl_timestamp):
            start_time = time()
            useful.run_sql_query(bigquery_client, table, params_)

            ########################################
            #   Gathering statistics
            ########################################
            # Calculate metrics
            dataset_ref = DatasetReference(project_id, dataset_id)
            table_ref = TableReference(dataset_ref, table)
            useful.build_statistics(project_id, dataset_id, table, bigquery_client,
                                    table_ref,
                                    start_time,
                                    dag_execution_date,
                                    etl_timestamp,
                                    statistics_table='statistics_analytics')
            ########################################

        return run_sql_query


    ##############################################################################################################

    # Service Account Info saved in Secret Manager for secure saving secret data
    secret_client = secretmanager_v1.SecretManagerServiceClient()
    response = secret_client.access_secret_version(
        request={"name": f"projects/{project_id}/secrets/{secret_id}/versions/latest"})

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(response.payload.data.decode("UTF-8")))
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    drive_service = build('drive', 'v3', credentials=credentials)

    ########################################
    # TASK 1: Creating etl_timestamp
    ########################################
    etl_timestamp = generate_etl_timestamp()

    ########################################
    # TASK 2: Creating BigQuery tables if not exists
    ########################################
    # BigQuery dataset ID
    dataset_id = 'data'
    for table_name, schema in schemas.items():
        create_table_task = create_table_task_factory(table_name, schema, dataset_id)
        created_table_task = create_table_task()
        start >> created_table_task >> etl_timestamp

    ########################################
    # TASK 3: Loading from Google Drive to BigQuery:
    #######################################
    # BigQuery dataset ID
    dataset_id = 'data'
    response = drive_service.files().list(q="mimeType='text/csv'").execute()
    for file in response.get('files', []):
        file_id = file.get('id')
        table_name = file.get('name').split('.')[0]
        schema_fields = schemas.get(table_name, [])
        load_task = drive_to_bq_task_factory(table_name, schema_fields, dataset_id)
        loaded_task = load_task(file_id, execution_date, etl_timestamp)
        etl_timestamp >> loaded_task >> unit_load_files

    ########################################
    # TASK 4: Creating Mapping Analytics table:
    ########################################

    # BigQuery dataset ID
    dataset_id = 'analytics'
    analytics_mapping = {
        "date_campaign_country_mapping_hash_key": {"project_id": project_id, "etl_timestamp": etl_timestamp},
        "app_user_mapping_hash_key": {"project_id": project_id, "etl_timestamp": etl_timestamp},
    }

    for table, params in analytics_mapping.items():
        analytics_mapping_run = analytics_mapping_task_factory(table, dataset_id)
        analytics_mapping_task = analytics_mapping_run(table, params, execution_date, etl_timestamp)
        unit_load_files >> analytics_mapping_task >> unit_analytics_mapping

    ########################################
    # TASK 5: Creating Analytics table:
    ########################################
    # BigQuery dataset ID
    dataset_id = 'analytics'
    analytics_table = {
        "user_installs": {"project_id": project_id, "etl_timestamp": etl_timestamp},
        "user_subscription_trials": {"project_id": project_id, "etl_timestamp": etl_timestamp},
        "app_rating": {"project_id": project_id, "etl_timestamp": etl_timestamp},
        "app_engagement_metrics": {"project_id": project_id, "etl_timestamp": etl_timestamp},
        "ua_campaign_spendings": {"project_id": project_id, "etl_timestamp": etl_timestamp},

        "merged_app_users": {"project_id": project_id, "etl_timestamp": etl_timestamp},
        "avg_session_by_country": {"project_id": project_id, "etl_timestamp": etl_timestamp},
    }

    for table, params in analytics_table.items():
        analytics_run = analytics_task_factory(table, dataset_id)
        analytics_task = analytics_run(table, params, execution_date, etl_timestamp)
        unit_analytics_mapping >> analytics_task >> unit_analytics


example_dag = create_and_load_tables()
