import sys
import warnings
import traceback
import logging
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, pandas_udf, udf, date_format
import pandas as pd
from elasticsearch import Elasticsearch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

model_path = "/app/models"  
tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(model_path, local_files_only=True)
sentiment_pipeline = pipeline(
    "text-classification",
    model=model,
    tokenizer=tokenizer,
    truncation=True,
    padding=True,
    max_length=128,
    return_all_scores=True
)

print("✅ Đã load xong mô hình từ:", model_path)

# Kết nối Elasticsearch
def create_elasticsearch_connection():
    try:
        es = Elasticsearch("http://elasticsearch:9200")
        if es.ping():
            logging.info("Kết nối Elasticsearch thành công")
        else:
            logging.warning("k thể ping Elasticsearch")
        return es
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error("Không thể kết nối Elasticsearch")
        return None


def check_if_index_exists(es, index_name="office_input"):
    if es.indices.exists(index=index_name):
        logging.info(f"Index '{index_name}' đã tồn tại.")
    else:
        es.indices.create(index=index_name)
        logging.info(f"✅ Đã tạo index '{index_name}' thành công.")
        

def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    from pyspark.sql import SparkSession
    try:
        # Spark session is established with elasticsearch and kafka jars. Suitable versions can be found in Maven repository.
        spark = (SparkSession.builder
                 .appName("Streaming Kafka-Spark")
                 .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.postgresql:postgresql:42.2.20")
                 .config("spark.driver.memory", "2048m")
                 .config("spark.sql.shuffle.partitions", 4)
                 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .config("spark.log.level", "ERROR")
                 .getOrCreate())
        logging.info('Spark session created successfully')
    except Exception:
        traceback.print_exc(file=sys.stderr) # To see traceback of the error.
        logging.error("Couldn't create the spark session")

    return spark

def create_initial_dataframe(spark_session):
    """
    Đọc dữ liệu streaming từ Kafka và tạo dataframe ban đầu.
    """
    try:
        # Đọc dữ liệu streaming từ topic 'news'
        # kafka.bootstrap.servers trỏ tới 'kafka:29092' là địa chỉ nội bộ trong mạng Docker
        df = spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "news") \
            .load()
        logging.info("Tạo dataframe ban đầu thành công")
    except Exception as e:
        logging.warning(f"Không thể tạo dataframe ban đầu vì lỗi: {e}")
        df = None

    return df


def create_final_dataframe(df):
    """
    Biến đổi dataframe ban đầu, parse JSON và tạo dataframe cuối cùng để hiển thị.
    """
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    from pyspark.sql.functions import from_json, col
    from pyspark.sql.functions import current_timestamp
    from pyspark.sql.types import TimestampType

    news_schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("link", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("published", StringType(), True)
    ])

    df_string = df.selectExpr("CAST(value AS STRING)")

    # Parse chuỗi JSON thành các cột riêng biệt dựa vào Schema
    df_parsed = df_string \
        .withColumn("data", from_json(col("value"), news_schema)) \
        .select("data.*")
    
    @pandas_udf("string")
    def predict_sentiment_udf(summaries: pd.Series) -> pd.Series:
        results = sentiment_pipeline(summaries.tolist())
        labels = []
        for res in results:
            best = max(res, key=lambda x: x["score"])
            labels.append(best["label"])
        return pd.Series(labels)

    df_final = df_parsed.withColumn("sentiment", predict_sentiment_udf(col("summary")))
    df_final = df_final.withColumn("timestamp", current_timestamp().cast(TimestampType()))

    return df_final

def start_all_streamings(final_df):
    """Chạy 1 streaming duy nhất và ghi đến nhiều đích"""
    logging.info(" Bắt đầu streaming")
    checkpointDir = "/tmp/spark_checkpoints"
    def write_to_multiple_sinks(batch_df, batch_id):
        try:
            # 1. Ghi ra console
            print(f"=== Batch {batch_id} ===")
            batch_df.show(truncate=False)
            
            # 2. Ghi vào Elasticsearch
            es_df = batch_df.withColumn(
                "timestamp",
                date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
            )
            if batch_id == 0:  # Chỉ tạo index cho batch đầu tiên
                es = create_elasticsearch_connection()
                if es:
                    check_if_index_exists(es, "office_input")
            
            (es_df.write
             .format("org.elasticsearch.spark.sql")
             .option("es.nodes", "elasticsearch")
             .option("es.port", "9200")
             .option("es.resource", "office_input")
             .mode("append")
             .save())
            
            # 3. Ghi vào PostgreSQL
            (batch_df.write
             .format("jdbc")
             .option("url", "jdbc:postgresql://postgres:5432/newsdb")
             .option("dbtable", "news_sentiment")
             .option("user", "spark")
             .option("password", "spark123")
             .option("driver", "org.postgresql.Driver")
             .mode("append")
             .save())
            
            logging.info(f" Đã xử lý batch {batch_id} thành công")
            
        except Exception as e:
            logging.error(f" Lỗi khi xử lý batch {batch_id}: {e}")

    query = (
        final_df.writeStream
        .outputMode("append")
        .foreachBatch(write_to_multiple_sinks)
        .option("checkpointLocation", checkpointDir)
        .start()
    )

    return query.awaitTermination()

if __name__ == '__main__':

    es = create_elasticsearch_connection()
    if es:
        check_if_index_exists(es, "office_input")

    spark_session = create_spark_session()

    if spark_session:
        initial_df = create_initial_dataframe(spark_session)

        if initial_df:
            final_df = create_final_dataframe(initial_df)
            start_all_streamings(final_df)

