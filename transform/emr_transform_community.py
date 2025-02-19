from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import conf
import psycopg2
import requests

def transform(data_source:str, output_uri:str, batch_period:str)-> None:
    with (
        SparkSession.builder.appName(f"transform news at {batch_period}").config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.sql.session.timeZone", "UTC")
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
            StructField('view_count', LongType(), True),
            StructField('like_count', LongType(), True),
            StructField('source', StringType(), True),
            StructField('link', StringType(), True),
            StructField('keyword', StringType(), True)
        ])

        df = spark.read.schema(data_schema).parquet(data_source + batch_period + '/')
        df = df.withColumnRenamed("keyword", "car_model")
        df = df.withColumn(
            'accident',
            F.array(*[
                F.when(F.col('title').contains(accident) | F.col('content').contains(accident), accident)
                for accident in conf.ACCIDENT_KEYWORD
            ])
        )

        # TEST: 모든 행을 급발진 사건으로 설정
        # df = df.withColumn(
        #     'accident',
        #     F.array(F.lit("급발진"))
        # )
        df_exploded = df.withColumn("accident", F.explode(F.col("accident"))) \
            .filter(F.col("accident").isNotNull())

        score_schema = df_exploded.schema.add('comm_score', IntegerType(), True)
        score_rdd = df_exploded.rdd.mapPartitions(partial(score_rdd_generator, param = conf.GPT))
        scored_df = score_rdd.toDF(score_schema).cache()

        grouped_df = scored_df.groupBy("car_model", "accident").agg(
            F.count("*").alias("count"),
            F.to_json(
                F.collect_list(
                    F.struct("content", "link")
                )
            ).alias("comm"),
            F.avg("comm_score").alias("avg_comm_score")
        )
        grouped_df.show()
        grouped_df.rdd.foreachPartition(partial(merge_batch_into_main_table, param = conf.RDS_PROPERTY))
        grouped_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
    return

def score_rdd_generator(partition, param):
    api_key = param["api_key"]
    model = param["model"]
    for row in partition:
        row_with_score = row.asDict().copy()
        try:
            row_with_score['comm_score'] = get_score_from_gpt(
                car_model=row_with_score['car_model'],
                accident=row_with_score['accident'],
                title = row['title'],
                content=row_with_score['content'],
                comment = row['comment'],
                api_key=api_key,
                model=model
            )
            yield row_with_score
        except Exception as e:
            print(f"Error processing row: {e}")
            row_with_score['comm_score'] = 0
            yield row_with_score

def get_score_from_gpt(car_model:str, accident:str, title:str, content: str, comment, api_key: str, model: str = "gpt-4o-mini") -> int:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": f"""
                [커뮤니티 분석 요청]

                [할 것]
                너는 자동차 관련 글에서 특정 차량과 사고 유형에 대한 감성 분석을 수행하는 AI야.
                주어진 데이터를 기반으로 **차량 모델과 사고 유형**(예: '{car_model}' + '{accident}')에 대한 전체적인 감성을 분석해줘.

                각 행에는 제목(title), 본문(content), 댓글(comment)들이 포함되어 있어.
                댓글은 ,로 구분되어 있어.
                이 모든 요소를 종합하여 **해당 차량과 사고 유형에 대한 감성 점수**를 100(매우 부정적)에서 0(매우 긍정적)까지의 범위로 숫자로 평가해줘.
                50에 가까울수록 중립적이며, 100에 가까울수록 부정적, 0에 가까울수록 긍정적인거야.
                반환은 반드시 0~100사이의 정수 숫자만 반환하고 아무런 말도 하지마.
                아래는 분석할 데이터야:

                ---
                **차량 모델:** {car_model}  
                **사고 유형:** {accident}  
                **제목:** {title}  
                **본문:** {content}  
                **댓글:** {comment}  
                
                ---
                """
            }
        ],
        "temperature": 0.2
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        content = data["choices"][0]["message"]["content"].strip()
        score = int(content)
        print(score)
        return score
    except Exception as e:
        print(f"Error while calling API: {e}. score set to 0.")
        return 0

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
