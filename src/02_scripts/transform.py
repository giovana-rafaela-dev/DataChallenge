import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import glob
import shutil
from dotenv import load_dotenv

load_dotenv()

def transform_users():
    raw_data_path = os.getenv('RAW_DATA_PATH')
    processed_data_path = os.getenv('PROCESSED_DATA_PATH')

    if not raw_data_path or not processed_data_path:
        raise ValueError("RAW_DATA_PATH and PROCESSED_DATA_PATH must be set")

    spark = SparkSession.builder \
        .appName("TransformUsers") \
        .master("local[*]") \
        .getOrCreate()

    try:
        df = spark.read.option("multiLine", "true").json(raw_data_path)
        users_df = df.select(explode(col("users")).alias("user"))
        
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

        output_dir = os.path.dirname(processed_data_path)
        os.makedirs(output_dir, exist_ok=True)
        
        temp_output = f"{output_dir}/temp_users"
        final_df.coalesce(1).write.mode("overwrite").csv(temp_output, header=True)
        
        csv_file = glob.glob(f"{temp_output}/part-*.csv")[0]
        shutil.move(csv_file, processed_data_path)
        shutil.rmtree(temp_output)
        
        print(f"Transform completed: {processed_data_path}")
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_users()
