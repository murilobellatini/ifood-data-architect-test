import os
import pyspark 
import findspark
import configparser
import pathlib as pl
from src.config import aws_secrets, RAW_DATA_PATH
from pyspark.sql import SparkSession


def create_pyspark_session(app_name:str='my_app', aws_secrets:dict=aws_secrets) -> SparkSession:
    """
    Returns configured PySparkSession. 
    """
    print('Starting PySpark session. Check your terminal for detailed logging...')

    findspark.init()
    config = configparser.ConfigParser()
    spark = pyspark.sql.SparkSession.builder \
                .appName(app_name) \
                .config("spark.memory.fraction", 0.8) \
                .config("spark.executor.memory", "8g") \
                .config("spark.driver.memory", "8g") \
                .config("spark.sql.shuffle.partitions" , "800") \
                .config('spark.sql.codegen.wholeStage', False) \
                .getOrCreate()

    # configures AWS S3 connection 
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_secrets.get('PUBLIC_KEY'))
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_secrets.get('SECRET_KEY'))

    print(f'PySpark session sucessfully created.')

    return spark

def ingest_data(s3_paths:list, spark:SparkSession, target_data_path:pl.Path=RAW_DATA_PATH) -> list:
    """
    Downloads data to `target_data_path` based on list of strings `s3_paths`.
    List of PySpark.DataFrame is returned with loaded data.
    """

    for s3_path in s3_paths:

        filename = s3_path.split('/')[-1]
        file_stem = filename.replace('.csv.gz', '').replace('.json.gz', '')
        output_path = RAW_DATA_PATH / file_stem

        if os.path.exists(output_path):
            print(f'Output already exists at `{output_path}`. Ingestion skipped for this file.')
            continue

        print(f'Ingestion started for `{s3_path}`')

        if s3_path.endswith('csv.gz'):
            df = spark.read.csv(s3_path, header=True)
        elif s3_path.endswith('json.gz'):
            df = spark.read.json(s3_path)
        else:
            print(f'File extension of `{s3_path}` does not correspond to readable DataFrame. Skipping file.')
            continue
        
        df.write.parquet(str(output_path))
        
        print(f'File successfully ingested into `{output_path}`')
