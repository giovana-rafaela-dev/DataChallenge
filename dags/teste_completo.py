"""
DAG de Teste Completo - DataChallenge
Testa funcionalidades básicas do Airflow e integração com scripts
"""

import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import logging

# Configurações padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Funções Python para teste
def teste_python():
    """Testa execução de função Python"""
    logging.info(" Função Python executada com sucesso!")
    logging.info(f"Data/Hora: {datetime.now()}")
    return "Python OK"

def teste_variaveis(**context):
    """Testa acesso a variáveis do Airflow"""
    ti = context.get('task_instance')
    dag = context.get('dag')
    logging.info(f" DAG ID: {dag.dag_id if dag else 'N/A'}")
    logging.info(f" Task ID: {ti.task_id if ti else 'N/A'}")
    logging.info(f" Run ID: {ti.run_id if ti else 'N/A'}")
    logging.info(f" Context keys: {list(context.keys())}")
    return "Variáveis OK"

def teste_xcom(**context):
    """Testa XCom para passar dados entre tasks"""
    ti = context['task_instance']
    valor_anterior = ti.xcom_pull(task_ids='python_task')
    logging.info(f"Valor recebido via XCom: {valor_anterior}")
    return "XCom OK"

# Definição da DAG
with DAG(
    dag_id='teste_completo_airflow',
    default_args=default_args,
    description='DAG completa para teste do ambiente Airflow',
    schedule=None,  # Execução manual
    start_date=pendulum.datetime(2026, 2, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['teste', 'validacao', 'datachallenge']
) as dag:
    
    # Task inicial
    inicio = EmptyOperator(
        task_id='inicio',
    )
    
    # Teste de operador Bash
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "✅ Bash funcionando! Data: $(date)"'
    )
    
    # Teste de operador Python
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=teste_python
    )
    
    # Teste de variáveis de contexto
    variaveis_task = PythonOperator(
        task_id='variaveis_task',
        python_callable=teste_variaveis
    )
    
    # Teste de XCom
    xcom_task = PythonOperator(
        task_id='xcom_task',
        python_callable=teste_xcom
    )
    
    # Teste de comandos do sistema
    info_sistema = BashOperator(
        task_id='info_sistema',
        bash_command='echo "Sistema: $OS" && echo "Usuário: $USERNAME" && echo "Dir: $(pwd)"'
    )
    
    # Teste de criação de arquivo
    criar_arquivo = BashOperator(
        task_id='criar_arquivo_teste',
        bash_command='echo "Teste Airflow - $(date)" > /tmp/teste_airflow.txt && cat /tmp/teste_airflow.txt'
    )
    
    # Task final
    fim = EmptyOperator(
        task_id='fim',
    )
    
    # Definindo ordem de execução
    inicio >> [bash_task, python_task, info_sistema]
    python_task >> variaveis_task >> xcom_task
    [bash_task, xcom_task, info_sistema] >> criar_arquivo >> fim
