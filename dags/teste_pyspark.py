"""
DAG de Teste - Integração PySpark com Airflow
Valida configuração e funcionamento do Apache Spark
"""

import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import logging

# Configurações padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def teste_import_pyspark():
    """Testa se o PySpark está instalado e pode ser importado"""
    try:
        import pyspark
        logging.info(f"PySpark versão: {pyspark.__version__}")
        return f"PySpark {pyspark.__version__} OK"
    except ImportError as e:
        logging.error(f"Erro ao importar PySpark: {e}")
        raise

def teste_spark_session():
    """Cria e testa uma SparkSession"""
    try:
        from pyspark.sql import SparkSession
        
        # Criar SparkSession
        spark = SparkSession.builder \
            .appName("TesteAirflowSpark") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        logging.info(f" SparkSession criada com sucesso!")
        logging.info(f"   App Name: {spark.sparkContext.appName}")
        logging.info(f"   Master: {spark.sparkContext.master}")
        logging.info(f"   Spark Version: {spark.version}")
        
        spark.stop()
        return "SparkSession OK"
        
    except Exception as e:
        logging.error(f" Erro ao criar SparkSession: {e}")
        raise

def teste_dataframe_basico():
    """Testa criação e manipulação de DataFrame"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit
        
        spark = SparkSession.builder \
            .appName("TesteDataFrame") \
            .master("local[*]") \
            .getOrCreate()
        
        # Criar DataFrame de teste
        dados = [
            (1, "João", 25, "SP"),
            (2, "Maria", 30, "RJ"),
            (3, "Pedro", 28, "MG"),
            (4, "Ana", 22, "SP"),
            (5, "Carlos", 35, "RJ")
        ]
        
        colunas = ["id", "nome", "idade", "estado"]
        df = spark.createDataFrame(dados, colunas)
        
        logging.info(" DataFrame criado com sucesso!")
        logging.info(f"   Total de registros: {df.count()}")
        logging.info(f"   Colunas: {df.columns}")
        
        # Testar operações básicas
        df_filtrado = df.filter(col("idade") > 25)
        count_filtrado = df_filtrado.count()
        logging.info(f"   Registros com idade > 25: {count_filtrado}")
        
        # Testar agregação
        df_agrupado = df.groupBy("estado").count()
        logging.info(f"   Agrupamento por estado: {df_agrupado.count()} grupos")
        
        # Mostrar dados
        logging.info("   Primeiros registros:")
        df.show()
        
        spark.stop()
        return f"DataFrame OK - {df.count()} registros processados"
        
    except Exception as e:
        logging.error(f" Erro ao manipular DataFrame: {e}")
        raise

def teste_leitura_escrita():
    """Testa operações de I/O com Spark"""
    try:
        from pyspark.sql import SparkSession
        import os
        
        spark = SparkSession.builder \
            .appName("TesteIO") \
            .master("local[*]") \
            .getOrCreate()
        
        # Criar dados de teste
        dados = [(i, f"valor_{i}", i * 10) for i in range(1, 101)]
        df = spark.createDataFrame(dados, ["id", "texto", "valor"])
        
        # Definir caminho temporário
        temp_path = "/tmp/teste_spark_airflow"
        
        # Escrever em formato Parquet
        df.write.mode("overwrite").parquet(temp_path)
        logging.info(f" Dados escritos em: {temp_path}")
        
        # Ler os dados de volta
        df_lido = spark.read.parquet(temp_path)
        count = df_lido.count()
        logging.info(f"Dados lidos com sucesso: {count} registros")
        
        # Validar
        assert count == 100, f"Esperado 100 registros, encontrado {count}"
        logging.info(" Validação I/O concluída com sucesso!")
        
        spark.stop()
        return f"I/O OK - {count} registros"
        
    except Exception as e:
        logging.error(f" Erro em I/O: {e}")
        raise

def teste_operacoes_avancadas():
    """Testa operações mais complexas do Spark"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, when, avg, sum as spark_sum
        
        spark = SparkSession.builder \
            .appName("TesteAvancado") \
            .master("local[*]") \
            .getOrCreate()
        
        # Criar dataset mais complexo
        dados = [
            (1, "Produto A", 100.0, 5, "Eletrônicos"),
            (2, "Produto B", 50.0, 10, "Livros"),
            (3, "Produto C", 200.0, 3, "Eletrônicos"),
            (4, "Produto D", 30.0, 20, "Livros"),
            (5, "Produto E", 150.0, 7, "Eletrônicos"),
        ]
        
        colunas = ["id", "produto", "preco", "quantidade", "categoria"]
        df = spark.createDataFrame(dados, colunas)
        
        # Operações complexas
        df_processado = df.withColumn(
            "total", col("preco") * col("quantidade")
        ).withColumn(
            "classificacao", 
            when(col("total") > 500, "Alto")
            .when(col("total") > 200, "Médio")
            .otherwise("Baixo")
        )
        
        logging.info(" Transformações aplicadas")
        df_processado.show()
        
        # Agregações
        resultado = df_processado.groupBy("categoria").agg(
            spark_sum("total").alias("total_vendas"),
            avg("preco").alias("preco_medio")
        )
        
        logging.info(" Agregações calculadas:")
        resultado.show()
        
        spark.stop()
        return "Operações Avançadas OK"
        
    except Exception as e:
        logging.error(f" Erro em operações avançadas: {e}")
        raise

def teste_sql():
    """Testa Spark SQL"""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("TesteSQL") \
            .master("local[*]") \
            .getOrCreate()
        
        # Criar dados
        dados = [
            ("João", 25, 5000),
            ("Maria", 30, 6000),
            ("Pedro", 28, 5500),
        ]
        
        df = spark.createDataFrame(dados, ["nome", "idade", "salario"])
        df.createOrReplaceTempView("funcionarios")
        
        # Executar SQL
        resultado = spark.sql("""
            SELECT 
                nome,
                idade,
                salario,
                CASE 
                    WHEN salario >= 6000 THEN 'Alto'
                    WHEN salario >= 5500 THEN 'Médio'
                    ELSE 'Padrão'
                END as nivel_salarial
            FROM funcionarios
            WHERE idade >= 25
            ORDER BY salario DESC
        """)
        
        logging.info(" Spark SQL executado com sucesso!")
        resultado.show()
        
        spark.stop()
        return "Spark SQL OK"
        
    except Exception as e:
        logging.error(f" Erro em Spark SQL: {e}")
        raise

# Definição da DAG
with DAG(
    dag_id='teste_pyspark_airflow',
    default_args=default_args,
    description='Teste completo de integração PySpark com Airflow',
    schedule=None,  # Execução manual
    start_date=pendulum.datetime(2026, 2, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['teste', 'pyspark', 'spark', 'validacao']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    # Teste 1: Importação
    task_import = PythonOperator(
        task_id='teste_import_pyspark',
        python_callable=teste_import_pyspark
    )
    
    # Teste 2: SparkSession
    task_session = PythonOperator(
        task_id='teste_spark_session',
        python_callable=teste_spark_session
    )
    
    # Teste 3: DataFrame básico
    task_dataframe = PythonOperator(
        task_id='teste_dataframe_basico',
        python_callable=teste_dataframe_basico
    )
    
    # Teste 4: I/O
    task_io = PythonOperator(
        task_id='teste_leitura_escrita',
        python_callable=teste_leitura_escrita
    )
    
    # Teste 5: Operações avançadas
    task_avancado = PythonOperator(
        task_id='teste_operacoes_avancadas',
        python_callable=teste_operacoes_avancadas
    )
    
    # Teste 6: Spark SQL
    task_sql = PythonOperator(
        task_id='teste_sql',
        python_callable=teste_sql
    )
    
    fim = EmptyOperator(task_id='fim')
    
    # Ordem de execução
    inicio >> task_import >> task_session
    task_session >> [task_dataframe, task_sql]
    task_dataframe >> task_io >> task_avancado >> fim
    task_sql >> fim
