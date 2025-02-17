from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import conf
import psycopg2
from functools import partial
from pyspark.sql.functions import regexp_replace, col

def transform(data_source:str, output_uri:str, batch_period:str)-> None:
    with (
        SparkSession.builder.appName(f"transform news at {batch_period}")
                # 로컬에서 코드를 실행시킬때 config 적용
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .getOrCreate()as spark
    ):
        data_schema = StructType([
            StructField('post_time', TimestampType(), True),
            StructField('title', StringType(), True),
            StructField('content', StringType(), True),
            StructField('source', StringType(), True),
            StructField('link', StringType(), True),
            StructField('keyword', StringType(), True)
        ])

        df = spark.read.schema(data_schema).parquet(data_source + batch_period + '/')
        df = df.withColumn(
            "content",
            regexp_replace(
                col("content"),
                "[^가-힣a-zA-Z0-9\\s]",
                ""
            )
        )

        df = df.withColumn(
            'accident',
            F.array(*[
                F.when(F.col('title').contains(accident) | F.col('content').contains(accident), accident)
                for accident in conf.ACCIDENT_KEYWORD
            ])
        )

        df_exploded = df.withColumn("accident", F.explode(F.col("accident"))) \
            .filter(F.col("accident").isNotNull())

        res_df = df_exploded.groupBy("keyword", "accident").agg(
            F.count("*").alias("count"),
            F.to_json(
                F.collect_list(
                    F.struct("content", "link")
                )
            ).alias("news")
        )

        res_df = res_df.select('keyword', 'accident', 'count', 'news')
        res_df.show()

        res_df.rdd.foreachPartition(partial(update_table_from_batch, param = conf.RDS_PROPERTY, batch_period = batch_period))
        res_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
        update_main_table(batch_period)

    return

def update_table_from_batch(partition, param, batch_period:str):
    conn = psycopg2.connect(
        dbname= param["dbname"],
        user=param["user"],
        password=param["password"],
        host=param["url"],
        port=param["port"],
    )
    cur = conn.cursor()
    start_batch_time, last_batch_time = batch_period.split('_')

    for row in partition:
        car_model = row['keyword']
        accident = row['accident']
        batch_count = row['count']
        news_json = row['news']
        query = f"""
        UPDATE accumulated_table
        SET start_batch_time = CASE
            WHEN news_acc_count = 0 THEN TO_TIMESTAMP('{start_batch_time}', 'YYYY-MM-DD-HH24-MI-SS')
            ELSE start_batch_time
        END,
        news_acc_count = news_acc_count + {batch_count} ,
        last_batch_time = TO_TIMESTAMP('{last_batch_time}', 'YYYY-MM-DD-HH24-MI-SS') ,
        news = jsonb_set(
            news,
            '{{news}}',
            COALESCE(news->'news', '[]'::jsonb) || '{news_json}'::jsonb )
        WHERE car_model = '{car_model}'
            AND accident = '{accident}';
        """
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()


def update_main_table(batch_period:str):
    start_batch_time, last_batch_time = batch_period.split('_')
    conn = psycopg2.connect(
        dbname= conf.RDS_PROPERTY["dbname"],
        user=conf.RDS_PROPERTY["user"],
        password=conf.RDS_PROPERTY["password"],
        host=conf.RDS_PROPERTY["url"],
        port=conf.RDS_PROPERTY["port"],
    )
    cur = conn.cursor()

    clear_alert_query = f"""
        UPDATE accumulated_table
        SET is_alert = FALSE;
    """

    clear_dead_issue_query = f"""
        UPDATE accumulated_table
        SET news_acc_count = 0,
            is_issue = FALSE,
            is_alert = FALSE,
            start_batch_time = NULL,
            last_batch_time = NULL,
            news = '{{"news":[]}}'::jsonb
        WHERE CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' - TO_TIMESTAMP('{start_batch_time}', 'YYYY-MM-DD-HH24-MI-SS') > '1 day';
    """

    set_issue_query = f""" 
        UPDATE accumulated_table
        SET is_issue = TRUE,
        is_alert = TRUE
        WHERE news_acc_count >= {conf.THRESHOLD} and is_issue = FALSE;
    """
    try:
        cur.execute(clear_alert_query)
        cur.execute(clear_dead_issue_query)
        cur.execute(set_issue_query)
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()

    conn.commit()
    cur.close()
    conn.close()



if __name__ == "__main__":

    # 로컬에서 사용 시 주석 해제
    data_source = conf.S3_NEWS_DATA
    output_uri = conf.S3_NEWS_OUTPUT
    batch_period = conf.S3_NEWS_BATCH_PERIOD
    transform(data_source, output_uri, batch_period)

    # EMR에서 실행할 때 주석 해제
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--data_source", help="s3 data uri")
    # parser.add_argument("--output_uri", help="s3 output uri")
    # parser.add_argument("--batch_period", help="batch period")
    # args = parser.parse_args()
    #
    # transform(args.data_source, args.output_uri, args.batch_period)

