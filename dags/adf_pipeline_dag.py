from airflow.hooks.base_hook import BaseHook
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.operators.dummy import DummyOperator 
from airflow.models.dag import DAG
from datetime import datetime, timedelta
import logging

# Retrieve connection details from Airflow
conn_id = "azure_data_factory"
connection = BaseHook.get_connection(conn_id)
extra = connection.extra_dejson  # Parse the Extra field as JSON

# Retrieve pipeline parameters from the Extra field
factory_name = extra.get("factory_name")
resource_group_name = extra.get("resource_group_name")

# Log the values of the variables to ensure they are correct
logging.info(f"Connection details: {connection}")
logging.info(f"Extra: {extra}")
logging.info(f"Factory name: {factory_name}")
logging.info(f"Resource group name: {resource_group_name}")

with DAG(
    dag_id = "automate_adf_pipeline",
    start_date = datetime(2025, 1, 4), #4th Jan
    schedule_interval = "@weekly", 
    catchup = False,
    default_args = {
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'conn_id' : 'azure_data_factory',
    },
    default_view = "graph",
) as dag:

    # Set dummy start & end values
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id = 'run_pipeline1',
        pipeline_name = "NASA Medallion Architecture",
        azure_data_factory_conn_id=conn_id,  
        factory_name=factory_name,  
        resource_group_name=resource_group_name,  
        wait_for_termination = True,
        on_execute_callback=lambda context: logging.info(f"Triggering ADF pipeline: {context['task_instance'].task_id}") 
    )

    begin >> run_pipeline1 >> end
