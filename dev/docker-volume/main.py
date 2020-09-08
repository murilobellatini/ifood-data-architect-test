from src.IOController import create_pyspark_session, ingest_data
from src.DataProcessor import create_trusted_order, create_trusted_order_items, create_trusted_status

raw_data_s3_paths = [
    's3n://ifood-data-architect-test-source/order.json.gz',
    's3n://ifood-data-architect-test-source/status.json.gz',
    's3n://ifood-data-architect-test-source/restaurant.csv.gz',
    's3n://ifood-data-architect-test-source/consumer.csv.gz'
    ]

if __name__ == "__main__":
    spark = create_pyspark_session()
    ingest_data(raw_data_s3_paths, spark)
    create_trusted_order(spark)
    create_trusted_order_items(spark)
    create_trusted_status(spark)
