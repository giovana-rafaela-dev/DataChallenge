
import os
import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from dotenv import load_dotenv

load_dotenv()

def transform_users():
    """Transforma dados de usuários: lê JSON, achata campos e salva CSV"""
    input_path = os.getenv("RAW_DATA_PATH")
    output_path = os.getenv("PROCESSED_DATA_PATH")
    
    if not input_path or not output_path:
        raise ValueError("Variáveis RAW_DATA_PATH e PROCESSED_DATA_PATH devem estar configuradas!")
    
    spark = SparkSession.builder \
        .appName("TransformUsers") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    try:
        # Ler JSON e explodir array users
        df_raw = spark.read.option("multiLine", "true").json(input_path)
        df = df_raw.select(explode(col("users")).alias("user")).select("user.*")
        
        # Transformar: achatar campos aninhados e remover senha
        df_transformed = df.select(
            col("id"), col("firstName"), col("lastName"), col("maidenName"),
            col("age"), col("gender"), col("email"), col("phone"), col("username"),
            col("birthDate"), col("bloodGroup"), col("height"), col("weight"), col("eyeColor"),
            col("hair.color").alias("hair_color"), col("hair.type").alias("hair_type"),
            col("address.address").alias("address_street"), col("address.city").alias("address_city"),
            col("address.state").alias("address_state"), col("address.stateCode").alias("address_state_code"),
            col("address.postalCode").alias("address_postal_code"), col("address.country").alias("address_country"),
            col("university")
        )
        # Salvar CSV
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        temp_output = output_path + "_temp"
        
        df_transformed.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(temp_output)
        # Renomear arquivo part-* para nome final
        csv_files = glob.glob(f"{temp_output}/part-*.csv")
        if csv_files:
            shutil.copy(csv_files[0], output_path)
            shutil.rmtree(temp_output)
        
        return output_path
        
    finally:
        spark.stop()
        
if __name__ == "__main__":
    transform_users()
