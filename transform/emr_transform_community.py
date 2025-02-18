from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import conf
import psycopg2

def transform(data_source:str, output_uri:str, batch_period:str)-> None:
    with (
        SparkSession.builder.appName(f"transform news at {batch_period}").config("spark.driver.bindAddress", "0.0.0.0")
                # 로컬에서 코드를 실행시킬때 config 적용
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .getOrCreate()as spark
    ):
        
        data_schema = StructType([
            StructField('post_time', TimestampType(), True),
            StructField('title', StringType(), True),
            StructField('content', StringType(), True),
            StructField('comment',  ArrayType(StringType()), True),
            StructField('viewCount', LongType(), True),
            StructField('likeCount', LongType(), True),
            StructField('source', StringType(), True),
            StructField('link', StringType(), True),
            StructField('keyword', StringType(), True)
        ])

        df = spark.read.schema(data_schema).parquet(data_source + batch_period + '/')
        # df = df.withColumn(
        #     'accident',
        #     F.array(*[
        #         F.when(F.col('title').contains(accident) | F.col('content').contains(accident), accident)
        #         for accident in conf.ACCIDENT_KEYWORD
        #     ])
        # )
        df = df.withColumn(
            'accident',
            F.array(F.lit("급발진"))
        )
        df_exploded = df.withColumn("accident", F.explode(F.col("accident"))) \
            .filter(F.col("accident").isNotNull())

        # 커뮤니티 데이터는 어차피 이슈인것만 가져오기 때문에 뉴스와 달리 groupby안해줘도됨.
        filtered_df = df_exploded.withColumnRenamed("keyword", "car_model")
        filtered_df = filtered_df.select('car_model', 'accident', 'post_time', 'title', 'content', 'comment', 'viewCount', 'likeCount')

        score_rdd = filtered_df.rdd.mapPartitions(partial(community_score_rdd_generator, param = conf.RDS_PROPERTY))
        score_df = score_rdd.toDF().groupBy("car_model", "accident").agg(
            F.count("*").alias("count"),
            F.avg("comm_score").alias("avg_comm_score")
        )
        score_df.rdd.foreachPartition(partial(merge_batch_into_main_table, param = conf.RDS_PROPERTY))
        score_df.show()
        filtered_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
    return

def community_score_rdd_generator(partition, param):
    for row in partition:
        row_with_score = row.asDict().copy()
        try:
            row_with_score['comm_score'] = get_community_score_from_gpt()
            yield row_with_score
        except Exception as e:
            print(f"Error executing query: {e}")
            row_with_score['comm_score'] = None
            yield row_with_score

def get_community_score_from_gpt()->int:
    return 1

def merge_batch_into_main_table(partition, param):
    conn = psycopg2.connect(
        dbname= param["dbname"],
        user=param["user"],
        password=param["password"],
        host=param["url"],
        port=param["port"],
    )
    cur = conn.cursor()

    for row in partition:
        query = f"""
        UPDATE accumulated_table
        SET comm_score = {row['avg_comm_score']} ,
            comm_acc_count = COALESCE(comm_acc_count, 0) + {row['count']}
        WHERE car_model = '{row['car_model']}'
            AND accident = '{row['accident']}';
        """
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":

    # 로컬에서 사용 시 주석 해제
    data_source = conf.S3_COMMUNITY_DATA
    output_uri = conf.S3_COMMUNITY_OUTPUT
    batch_period = conf.S3_COMMUNITY_BATCH_PERIOD
    transform(data_source, output_uri, batch_period)

    # EMR에서 실행할 때 주석 해제
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--data_source", help="s3 data uri")
    # parser.add_argument("--output_uri", help="s3 output uri")
    # parser.add_argument("--batch_period", help="batch period")
    # args = parser.parse_args()
    #
    # transform(args.data_source, args.output_uri, args.batch_period)
