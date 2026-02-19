import os  # (caminhos, diretórios)
from pyspark.sql import SparkSession  # Importa SparkSession para criar contexto Spark
from pyspark.sql.functions import col, explode  # col: referencia colunas, explode: desaninha arrays
import glob  # Busca arquivos com padrões (wildcards)
import shutil  # Operações de alto nível em arquivos (mover, copiar, deletar)
from dotenv import load_dotenv  # Carrega variáveis de ambiente de arquivo .env

load_dotenv()  # Executa função que lê o arquivo .env

def transform_users():  # Define função principal de transformação
    raw_data_path = os.getenv('RAW_DATA_PATH')  # Busca caminho do JSON bruto
    processed_data_path = os.getenv('PROCESSED_DATA_PATH')  # Busca caminho do CSV final

    if not raw_data_path or not processed_data_path:  # Valida se variáveis foram definidas
        raise ValueError("RAW_DATA_PATH and PROCESSED_DATA_PATH must be set")  # Lança erro se alguma estiver vazia

    spark = SparkSession.builder \
        .appName("TransformUsers") \
        .master("local[*]") \
        .getOrCreate()  # Cria nova sessão ou retorna existente

    try:  # Inicia bloco de tratamento de exceções
        df = spark.read.option("multiLine", "true").json(raw_data_path)  # Lê JSON com múltiplas linhas
        users_df = df.select(explode(col("users")).alias("user"))  # Explode array "users" em múltiplas linhas, cria coluna "user"
        
        final_df = users_df.select(  
            col("user.id").alias("id"),  
            col("user.firstName").alias("firstName"),  
            col("user.lastName").alias("lastName"),  
            col("user.maidenName").alias("maidenName"),  
            col("user.age").alias("age"),  
            col("user.gender").alias("gender"), 
            col("user.email").alias("email"),   
            col("user.phone").alias("phone"), 
            col("user.username").alias("username"), 
            col("user.birthDate").alias("birthDate"), 
            col("user.image").alias("image"),  
            col("user.bloodGroup").alias("bloodGroup"),  
            col("user.height").alias("height"),  
            col("user.weight").alias("weight"),  
            col("user.eyeColor").alias("eyeColor"),
            col("user.hair.color").alias("hair_color"),  
            col("user.hair.type").alias("hair_type"), 
            col("user.ip").alias("ip"),  
            col("user.address.address").alias("address_address"),  
            col("user.address.city").alias("address_city"),  
            col("user.address.state").alias("address_state"),  
            col("user.address.stateCode").alias("address_stateCode"), 
            col("user.address.postalCode").alias("address_postalCode")  
        )

        output_dir = os.path.dirname(processed_data_path)  # Extrai diretório do caminho completo
        os.makedirs(output_dir, exist_ok=True)  # Cria diretório se não existir
        
        temp_output = f"{output_dir}/temp_users"  # Define pasta temporária para escrita Spark
        final_df.coalesce(1).write.mode("overwrite").csv(temp_output, header=True)  # coalesce(1): gera 1 arquivo só, mode overwrite: sobrescreve, header: inclui cabeçalho
        
        csv_file = glob.glob(f"{temp_output}/part-*.csv")[0]  # Busca arquivo CSV gerado pelo Spark (nome automático part-xxxxx.csv)
        shutil.move(csv_file, processed_data_path)  # Move e renomeia arquivo CSV para destino final
        shutil.rmtree(temp_output)  # Remove pasta temporária e todo seu conteúdo
        
        print(f"Transform completed: {processed_data_path}")  # Log de sucesso com caminho do arquivo
    finally:  # Bloco executado sempre, mesmo com erro
        spark.stop()  # Finaliza sessão Spark e libera recursos

if __name__ == "__main__":  # Verifica se script está sendo executado diretamente
    transform_users()  # Executa função de transformação
