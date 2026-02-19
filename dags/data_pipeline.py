from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys
import importlib.util

sys.path.append('/opt/airflow/src')

# Importar scripts com números nos nomes
spec_extract = importlib.util.spec_from_file_location("extract", "/opt/airflow/src/scripts_extract/extract.py")
extract_module = importlib.util.module_from_spec(spec_extract)
spec_extract.loader.exec_module(extract_module)

spec_transform = importlib.util.spec_from_file_location("transform", "/opt/airflow/src/scripts_transform/transform.py")
transform_module = importlib.util.module_from_spec(spec_transform)
spec_transform.loader.exec_module(transform_module)

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Não depende de execuções anteriores
    'retries': 1,              # Tenta novamente 1 vez se falhar
}
# Criar a DAG
with DAG(
    dag_id='data_pipeline_users',
    default_args=default_args,
    description='ETL pipeline: Extract from DummyJSON API and Transform with PySpark',
    schedule=None,  # Execução manual 
    start_date=datetime(2026, 2, 1),
    catchup=False,  # Nã roda datas anteriores
    tags=['etl', 'dummyjson', 'pyspark']
) as dag:
    # TASK 1: Extract
    extract_task = PythonOperator(
        task_id='extract_users_from_api',
        python_callable=extract_module.extract_users,
    )
    # TASK 2: Transform
    transform_task = PythonOperator(
        task_id='transform_users_to_csv',
        python_callable=transform_module.transform_users,
    )
    # Definir ordem de execução
    extract_task >> transform_task
