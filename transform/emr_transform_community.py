from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import conf

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
        df = df.withColumn(
            'accident',
            F.array(*[
                F.when(F.col('title').contains(accident) | F.col('content').contains(accident), accident)
                for accident in conf.ACCIDENT_KEYWORD
            ])
        )

        df_exploded = df.withColumn("accident", F.explode(F.col("accident"))) \
            .filter(F.col("accident").isNotNull())

        # 커뮤니티 데이터는 어차피 이슈인것만 가져오기 때문에 뉴스와 달리 groupby안해줘도됨.
        res_df = df_exploded.withColumnRenamed("keyword", "car_model")
        res_df = res_df.select('car_model', 'accident', 'post_time', 'title', 'content', 'comment', 'viewcount', 'likecount')
        res_df.show()
        
        batch_period = 'T'.join(batch_period.split(' '))
        res_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
    return

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
