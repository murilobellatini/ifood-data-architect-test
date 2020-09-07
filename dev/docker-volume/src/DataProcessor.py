from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from src.config import TRUSTED_DATA_PATH, RAW_DATA_PATH
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, FloatType
from pyspark.sql.functions import from_json, explode, flatten, col, rank, col, monotonically_increasing_id, desc, first


def create_trusted_status(spark:SparkSession) -> DataFrame:
    """
    Creates requested Status dataset for Datamart
    """

    print('Starting processing to generate Status dataset...')

    df = spark.read.parquet(str(RAW_DATA_PATH / 'status'))

    df = df.dropDuplicates()\
           .groupBy("order_id")\
           .pivot("value")\
           .agg(first("created_at"))

    print(f'Exporting dataset to file system...')

    output_path = TRUSTED_DATA_PATH / 'order'
    tmp.write.parquet(str(output_path))

    print(f'Dataset sucessfully exported to `{output_path}`!')

    return df


def create_trusted_order(spark:SparkSession) -> DataFrame:
    """
    Creates requested Order dataset for Datamart
    """

    print('Starting processing to generate Order Items dataset...')

    o_df = load_sanitized_dataframe('order', spark)
    c_df = load_sanitized_dataframe('consumer', spark)
    r_df = load_sanitized_dataframe('restaurant', spark)
    s_df = load_sanitized_dataframe('status', spark)

    tmp = (o_df
        .join(c_df, on='customer_id', how='left')
        .join(r_df, on='merchant_id', how='left')
        .join(s_df, on='order_id', how='left')
        .dropDuplicates()
        )

    print(f'Exporting dataset to file system...')

    output_path = TRUSTED_DATA_PATH / 'order'

    # anonymize sensitive data by dropping columns
    sensitive_data_columns = ['order_cpf', 'order_customer_name', 'consumer_customer_name', 'consumer_customer_phone_number']
    tmp = tmp.drop(*sensitive_data_columns)

    # fix dataset data types
    tmp = fix_order_dtypes(tmp)

    # exports data partinioned by merchant's time at order creation
    tmp.write.partitionBy('order_order_created_at').parquet(str(output_path))

    print(f'Dataset sucessfully exported to `{output_path}`!')

    return tmp
    
def create_trusted_order_items(spark:SparkSession) -> DataFrame:
    """
    Creates requested Order Items dataset for Datamart
    """

    print('Starting processing to generate Order Items dataset...')
    
    df = spark.read.parquet(str(RAW_DATA_PATH / 'order'))

    schema = ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("addition", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("quantity", FloatType(), True),
        StructField("sequence", FloatType(), True),
        StructField("unitPrice", StringType(), True),
        StructField("externalId", StringType(), True),
        StructField("totalValue", StringType(), True),
        StructField("customerNote", StringType(), True),
        StructField("garnishItems", StringType(), True),
        StructField("integrationId", StringType(), True),
        StructField("totalAddition", StringType(), True),
        StructField("totalDiscount", StringType(), True),
    ]))

    df = df.withColumn("items", from_json(df["items"], schema))
    tmp = df.select('order_id', explode(df['items']).alias('items'))
    tmp = tmp.select('order_id', 'items.*')
    tmp = tmp.dropDuplicates()
    output_path = TRUSTED_DATA_PATH / 'order_items'

    print(f'Exporting dataset to file system...')
    
    tmp.write.parquet(str(output_path))

    print(f'Dataset sucessfully exported to `{output_path}`!')

    return tmp

def explore_dataframe(df:DataFrame) -> None:
    """
    Checks shape and schema of DataFrame
    """
    print('(#rows, #columns) =', (df.count(), len(df.columns)))
    return df.printSchema()

def fix_dataframe_dtypes(df:DataFrame, dtypes:dict) -> DataFrame:
    """
    Returns DataFrame `df` with corrected schema based on dtypes
    """
    for dtype, cols in dtypes.items():
        for col in cols:
            df = df.withColumn(col, df[col].cast(dtype))

    return df

def fix_order_dtypes(df:DataFrame) -> DataFrame:
    """
    Adjusts data types of Order dataset (data validation)
    """

    dtypes = {
        'float': [
            'order_delivery_address_latitude', 'order_delivery_address_longitude', 'order_merchant_latitude',
            'order_merchant_longitude', 'order_order_total_amount', 'restaurant_price_range',
            'restaurant_average_ticket', 'restaurant_takeout_time', 'restaurant_delivery_time'],
        'bigint': [
            'order_delivery_address_zip_code', 'restaurant_merchant_zip_code'
        ]}

    df = fix_schema(df, dtypes)
    
    return df

def extract_latest_values(df:DataFrame, id_col:str, dt_col:str) -> DataFrame:
    """
    Returns DataFrame after dropping duplicates of column `id_col` and
    keeping the lastest value based on timestamp column `dt_col`
    """

    window = Window.partitionBy(id_col).orderBy(desc(dt_col),'tiebreak')

    df = df.withColumn('tiebreak', monotonically_increasing_id()) \
           .withColumn('rank', rank().over(window)) \
           .filter(col('rank') == 1).drop('rank','tiebreak')

    return df

def load_sanitized_dataframe(table:str, spark:SparkSession) -> DataFrame:
    """
    Loads DataFrame into standard for produced joind datamart dataset `Order`.
    """
    df = spark.read.parquet(str(RAW_DATA_PATH / table))
    df = add_prefix(df, table)

    if table == 'restaurant':
        df = df.withColumnRenamed('id', 'merchant_id')

    if table == 'status':
        df = extract_latest_values(df=df, id_col='order_id', dt_col='status_created_at')
        
    return df

def add_prefix(df:DataFrame, prefix:str, skip_ids:bool=True) -> DataFrame:
    """
    Adds prefix to every columns on `df` except `id` for better consistency.
    """

    for col in df.columns:

        if col.endswith('id'):
            continue

        df = df.withColumnRenamed(col, f'{prefix}_{col}')

    return df
