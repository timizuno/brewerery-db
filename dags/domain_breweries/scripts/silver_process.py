from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from datetime import datetime
import logging

def process_silver_layer():
    """
        Read the raw data from the bronze layer with the actual date (csv),
        process the data cleaning and transformation with pyspark and saves as
        parquet, partitioned by city in the silver layer.
    """

    current_date = datetime.today().strftime('%Y%m%d')
    input_path = f'/opt/airflow/data/bronze/{current_date}_extraction_breweries.csv'
    output_path = f'/opt/airflow/data/silver/breweries/'

    logging.info(f'Starting the silver transformation for the file: {input_path}')
    spark = SparkSession.builder.appName('silver_local').getOrCreate()

    schema = StructType([
        StructField('id', StringType(), True), 
        StructField('name', StringType(), True), 
        StructField('brewery_type', StringType(), True), 
        StructField('address_1', StringType(), True), 
        StructField('address_2', StringType(), True), 
        StructField('address_3', StringType(), True), 
        StructField('city', StringType(), True), 
        StructField('state_province', StringType(), True), 
        StructField('postal_code', StringType(), True), 
        StructField('country', StringType(), True), 
        StructField('longitude', DoubleType(), True), 
        StructField('latitude', DoubleType(), True), 
        StructField('phone', StringType(), True), 
        StructField('website_url', StringType(), True), 
        StructField('state', StringType(), True), 
        StructField('street', StringType(), True)
    ])

    try:
        df_silver = spark.read.csv(input_path,header=True, schema=schema)\
            .filter(col('id').isNotNull())\
            .filter(col('city').isNotNull())
        logging.info(f'Read {df_silver.count()} rows in the bronze layer.')

        logging.info('Saving the processed data, partitioned by city.')
        df_silver.write.partitionBy('city').mode('overwrite').parquet(output_path)

    finally:
        spark.stop()
        logging.info('Spark session terminated.')