from src.config import TRUSTED_DATA_PATH, RAW_DATA_PATH
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, FloatType
from pyspark.sql.functions import from_json, explode, flatten, col, rank, col, monotonically_increasing_id, desc


def explore_dataframe(df:DataFrame):
    """
    Checks shape and schema of DataFrame
    """
    print('(#rows, #columns) =', (df.count(), len(df.columns)))
    return df.printSchema()

def fix_schema(df:DataFrame, dtypes:dict):
    """
    Return  DataFrame `df` with corrected schema based on dtypes
    """
    for dtype, cols in dtypes.items():
        for col in cols:
            df = df.withColumn(col, df[col].cast(dtype))

    return df

def fix_order_schema(df):

    dtypes = {
        'float': [
            'delivery_address_latitude', 'delivery_address_longitude',
            'merchant_latitude', 'merchant_longitude', 'order_total_amount'],
        'bigint': [
            'cpf', 'delivery_address_zip_code']
    }

    df = fix_schema(df, dtypes)
    
    return df

def create_trusted_order_items(spark:SparkSession):
    """
    Creates requested Order Items table based on raw Orders `df`.
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

    print(f'Exporting dataset file system...')
    
    tmp.write.parquet(str(output_path))

    print(f'Dataset sucessfully exported to `{output_path}`!')

    return tmp

def extract_latest_values(df:DataFrame, id_col:str, dt_col:str):
    """
    Returns DataFrame after dropping duplicates of column `id_col` and
    keeping the lastest value based on timestamp column `dt_col`
    """

    window = Window.partitionBy(id_col).orderBy(desc(dt_col),'tiebreak')

    df = df.withColumn('tiebreak', monotonically_increasing_id()) \
           .withColumn('rank', rank().over(window)) \
           .filter(col('rank') == 1).drop('rank','tiebreak')

    return df

def load_sanitized_dataframe(table:str, spark:SparkSession):
    df = spark.read.parquet(str(RAW_DATA_PATH / table))
    df = add_prefix(df, table)

    if table == 'restaurant':
        df = df.withColumnRenamed('id', 'merchant_id')

    if table == 'status':
        df = extract_latest_values(df=df, id_col='order_id', dt_col='status_created_at')
        
    return df

def create_trusted_order(spark:SparkSession):

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

    print(f'Exporting dataset file system...')

    output_path = TRUSTED_DATA_PATH / 'order'

    # anonymize sensitive data by dropping columns
    sensitive_data_columns = ['cpf', 'customer_name', 'consumer_customer_name', 'consumer_customer_phone_number']
    tmp = tmp.drop(*sensitive_data_columns)

    tmp.write.parquet(str(output_path))

    print(f'Dataset sucessfully exported to `{output_path}`!')

    return tmp

def add_prefix(df:DataFrame, prefix:str, skip_ids:bool=True):

    for col in df.columns:

        if col.endswith('id'):
            continue

        df = df.withColumnRenamed(col, f'{prefix}_{col}')

    return df
