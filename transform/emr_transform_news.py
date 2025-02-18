from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import conf
import psycopg2
from functools import partial
from pyspark.sql.functions import regexp_replace, col
import requests
import json

def transform(data_source:str, output_uri:str, batch_period:str)-> None:
    with (
        SparkSession.builder.appName(f"transform news at {batch_period}")
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
            StructField('source', StringType(), True),
            StructField('link', StringType(), True),
            StructField('keyword', StringType(), True)
        ])

        df = spark.read.schema(data_schema).parquet(data_source + batch_period + '/')
        df = df.withColumnRenamed("keyword", "car_model").select('car_model', 'title', 'content', 'link')
        df = df.withColumn(
            "content",
            regexp_replace(
                col("content"),
                "[^가-힣a-zA-Z0-9\\s]",
                ""
            )
        )

        accident_df = df.withColumn(
            'accident',
            F.array(*[
                F.when(F.col('title').contains(accident) | F.col('content').contains(accident), accident)
                for accident in conf.ACCIDENT_KEYWORD
            ])
        )

        df_exploded = accident_df.withColumn("accident", F.explode(F.col("accident"))) \
            .filter(F.col("accident").isNotNull())

        score_schema = df_exploded.schema.add('news_score', IntegerType(), True)
        score_rdd = df_exploded.rdd.mapPartitions(partial(score_rdd_generator, param = conf.GPT))
        scored_df = score_rdd.toDF(score_schema).cache()

        grouped_df = scored_df.groupBy("car_model", "accident").agg(
            F.count("*").alias("count"),
            F.to_json(
                F.collect_list(
                    F.struct("content", "link")
                )
            ).alias("news"),
            F.avg("news_score").alias("avg_news_score")  # news_score의 평균값 계산
        )
        grouped_df.show()

        grouped_df.rdd.foreachPartition(partial(merge_batch_into_main_table, param = conf.RDS_PROPERTY, batch_period = batch_period))
        grouped_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
        update_main_table(batch_period)

    return

def score_rdd_generator(partition, param):
    api_key = param["api_key"]
    model = param["model"]
    for row in partition:
        row_with_score = row.asDict().copy()
        try:
            row_with_score['news_score'] = get_score_from_gpt(
                car_model=row_with_score['car_model'],
                accident=row_with_score['accident'],
                input_text=row_with_score['content'],
                api_key=api_key,
                model=model
            )
            yield row_with_score
        except Exception as e:
            print(f"Error processing row: {e}")
            row_with_score['news_score'] = 0
            yield row_with_score


def merge_batch_into_main_table(partition, param, batch_period:str):
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
        car_model = row['car_model']
        accident = row['accident']
        batch_count = row['count']
        news_json = row['news']
        news_score = row['avg_news_score']
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
            COALESCE(news->'news', '[]'::jsonb) || '{news_json}'::jsonb ) ,
        news_score = {news_score}
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

    clear_alert_column_query = f"""
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
            news_score = 0,
            comm_score = 0,
            issue_score = 0,
            comm_acc_count = 0,
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
        cur.execute(clear_alert_column_query)
        cur.execute(clear_dead_issue_query)
        cur.execute(set_issue_query)
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

def get_score_from_gpt(car_model:str, accident:str, input_text: str, api_key: str, model: str = "gpt-4o-mini") -> int:
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
                [뉴스 분석 요청]
                {input_text}

                [할 것]
                - 위의 뉴스 내용들은 현대자동차 {car_model} 모델의 {accident} 관련한 이슈, 사고를 다루고 있는 내용들이고, 한 이슈에 대한 여러 기사들을 너에게 입력해줬어.
                - 위의 뉴스 내용을 기반으로 아래 작업을 수행해줘.
                - 답변은 반드시 한글로 작성되어야 해.
                - 해당 뉴스에서 아래의 기준에 따라 정수형으로 점수만 반환
                >> 이슈 심각도: 해당 뉴스가 현대자동차의 브랜드 이미지에 얼마나 타격을 줄만한 뉴스인지 수치화해줘.
                    - 이슈 심각도를 평가할 때 이슈 심각도를 평가할 때 주로 고려해야 하는 요소들은 다음과 같아.
                        - 현대차의 직접적 책임 여부 : 차량 결함 vs 운전자 과실
                        - 사고 규모 : 인명 피해 및 사고 차량 수
                        - 언론 및 소비자 반응 : 부정적 보도, 여론 형성 여부
                        - 법적 규제 리스크 : 리콜, 소송, 정부 개입 가능성
                    - 점수 산정 기준은 다음과 같아.
                        - 80점 ~ 100점(최악의 상황, 강력한 대응 필수)
                            - 현대차의 직접적인 책임 : 차량 결함이 명백하고 심각함
                            - 사고 규모 : 다수의 사망, 중상 사고 발생. 글로벌 뉴스화
                            - 언론 및 소비자 반응 : 부정적 여론 폭발, 불매 운동 조짐
                                - 법적 규제 리스크 : 대규모 리콜, 국가 차원의 조사 착수
                            - 60점 ~ 80점(매우 심각, 신속한 대응 필요)
                                - 현대차의 책임 가능성 높음 : 일부 논란 있지만 확실한 증거 부족
                                - 사고 규모 : 사망자는 없지만 중대 사고 발생
                                - 언론 및 소비자 반응 : 부정적 여론 확산, 브랜드 신뢰 하락 우려
                                - 법적 규제 리스크 : 리콜 가능성 높음, 당국 조사 진행
                            - 40점 ~ 60점(주의 요망, 전략적 대응 필요)
                                - 현대차의 책임 불분명 : 운전자 과실 가능성 존재
                                - 사고 규모 : 일부 차량 손상 및 경상자 발생
                                - 언론 및 소비자 반응 : 논란 있지만 대중적 관심 크지 않음
                                - 법적 규제 리스크 : 리콜 가능성 낮음, 조사는 진행될 수 있음
                            - 20점 ~ 40점(경미한 이슈, 모니터링 필요)
                                - 현대차의 책임 낮음 : 외부 요인 가능성 높음
                                - 사고 규모 : 일부 차량 문제지만 대형 사고 아님
                                - 언론 및 소비자 반응 : 국지적 논란, 이슈 확산 가능성 낮음
                                - 법적 규제 리스크 : 리콜 필요 없음, 단순 보상 차원 해결 가능
                            - 0점 ~ 20점(무시 가능, 영향 미미)
                                - 현대차의 책임 거의 없음 : 소비자 또는 외부 요인으로 판명
                                - 사고 규모 : 개별 사례이며 확대 가능성 없음
                                - 언론 및 소비자 반응 : 온라인 커뮤니티 불만 수준, 확산 가능성 적음
                                - 법적 규제 리스크 : 리콜, 조사 필요 없음
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

