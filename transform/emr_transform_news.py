import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import conf

def transform(data_source:str, output_uri:str)-> None:
    with (
        SparkSession.builder.appName("EMR transform news")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .getOrCreate()as spark
    ):
        data_schema = StructType([
            StructField('post_time', LongType(), True),
            StructField('title', StringType(), True),
            StructField('content', StringType(), True),
            StructField('source', StringType(), True),
            StructField('link', StringType(), True),
            StructField('keyword', StringType(), True)
        ])

        df = spark.read.schema(data_schema).parquet(data_source)
        df = df.withColumn(
            'accident',
            F.array(*[
                F.when(F.col('title').contains(accident) | F.col('content').contains(accident), accident)
                for accident in conf.ACCIDENT_KEYWORD
            ])
        )
        # df.show()

        df_exploded = df.withColumn("accident", F.explode(F.col("accident"))).filter(
            F.col("accident").isNotNull()
        )

        res_df = df_exploded.groupBy("accident").agg(
            F.count("*").alias("count"),
            F.collect_list("content").alias("contents")
        )
        res_df = res_df.withColumn("car", F.lit(df.select('keyword').first()[0]))
        res_df = res_df.select('car', 'accident', 'count', 'contents')
        res_df.show()
        # res_df.write.mode('overwrite').parquet(output_uri)
    return

if __name__ == "__main__":

    # 로컬에서 사용 시 주석 해제
    data_source = conf.S3_NEWS_DATA
    output_uri = conf.S3_NEW_OUTPUT
    transform(data_source, output_uri)

    # EMR에서 실행할 때 주석 해제
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--data_source", help="s3 uri")
    # parser.add_argument("--output_uri", help="s3 uri")
    # args = parser.parse_args()
    #
    # transform(args.data_source, args.output_uri)
