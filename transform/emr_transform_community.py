from functools import partial
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
# import conf
import urllib.request
import json
import argparse
import ast
import base64
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed # 멀티스레딩
from collections import defaultdict
from pyspark.sql import Row

# ✅ 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform(data_source:str, output_uri:str, batch_period:str, community_accident_keyword:str, gpt, issue_list)-> None:
    with (
        SparkSession.builder.appName(f"transform news at {batch_period}").config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.sql.session.timeZone", "UTC")
                # 로컬에서 코드를 실행시킬때 config 적용
                # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .getOrCreate()as spark
    ):
        
        data_schema = StructType([
            StructField('post_time', TimestampType(), True),
            StructField('title', StringType(), True),
            StructField('content', StringType(), True),
            StructField('view_count', LongType(), True),
            StructField('like_count', LongType(), True),
            StructField('source', StringType(), True),
            StructField('link', StringType(), True)
        ])

        # base64 디코딩
        issue_list = json.loads(base64.b64decode(issue_list).decode('utf-8'))
        community_accident_keyword = json.loads(base64.b64decode(community_accident_keyword).decode('utf-8'))

        issue_list = ast.literal_eval(issue_list)
        community_accident_keyword = ast.literal_eval(community_accident_keyword)
        logger.info(f"formatted issue_list: {issue_list}")
        logger.info(f"formatted community_accident_keyword: {community_accident_keyword}")


        df = spark.read.schema(data_schema).parquet(data_source + batch_period + '/')

        
        # issue_list에 해당하는 사고 유형만 추출
        selected_keywords = {issue['accident']: community_accident_keyword[issue['accident']] for issue in issue_list if issue['accident'] in community_accident_keyword}
        logger.info(f"selected_keywords: {selected_keywords}")

        # 🔹 `car_model` 매칭을 위한 딕셔너리 생성 (accident → car_model 매핑)
        accident_to_car_model = defaultdict(set)
        for issue in issue_list:
            accident_to_car_model[issue['accident']].add(issue['car_model'])
        
        # set을 list로 변환
        accident_to_car_model = {k: list(v) for k, v in accident_to_car_model.items()}

        logger.info(f"accident_to_car_model: {accident_to_car_model}")

        # 이부분 바뀌어야 함.
        df = df.withColumn(
            'accident',
            F.array_distinct(F.array(*[
                F.when(F.col('title').contains(subkey) | F.col('content').contains(subkey), accident)
                for accident, subkeys in selected_keywords.items()
                for subkey in subkeys
            ]))
        )
        df.show()

        # TEST: 모든 행을 급발진 사건으로 설정
        # df = df.withColumn(
        #     'accident',
        #     F.array(F.lit("급발진"))
        # )
        df_exploded = df.withColumn("accident", F.explode(F.col("accident"))) \
            .filter(F.col("accident").isNotNull())
        
        # 🚀 `accident_to_car_model`을 Spark DataFrame으로 변환
        mapping_data = [(accident, car_model) for accident, car_models in accident_to_car_model.items() for car_model in car_models]
        mapping_df = spark.createDataFrame([Row(accident=a, car_model=c) for a, c in mapping_data])

        # 🚀 `accident` 기준으로 `join`
        df_exploded = df_exploded.join(mapping_df, on="accident", how="left")

        # 🔹 `car_model`이 issue_list에 있는 자동차 모델만 필터링
        # car_models = [issue["car_model"] for issue in issue_list]
        # df_filtered = df_exploded.filter(F.col("car_model").isin(car_models))
        

        score_schema = df_exploded.schema.add('comm_score', IntegerType(), True)
        gpt = json.loads(gpt)
        score_rdd = df_exploded.rdd.mapPartitions(partial(score_rdd_generator, param = gpt))
        scored_df = score_rdd.toDF(score_schema).cache()
        scored_df.show()

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

        grouped_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
    return

# 수정 전 함수
# def score_rdd_generator(partition, param):
#     api_key = param["api_key"]
#     model = param["model"]
#     for row in partition:
#         row_with_score = row.asDict().copy()
#         try:
#             row_with_score['comm_score'] = get_score_from_gpt(
#                 car_model=row_with_score['car_model'],
#                 accident=row_with_score['accident'],
#                 title = row_with_score['title'],
#                 content=row_with_score['content'],
#                 api_key=api_key,
#                 model=model
#             )
#             yield row_with_score
#         except Exception as e:
#             print(f"Error processing row: {e}")
#             row_with_score['comm_score'] = 0
#             yield row_with_score

# 수정 후 함수
def score_rdd_generator(partition, param):
    api_key = param["api_key"]
    model = param["model"]

    partition_list = list(partition)  # 🔥 partition을 리스트로 변환 (멀티쓰레딩 적용 가능)

    if not partition_list:
        return iter([])  # 빈 partition 처리

    # ✅ 🔥 병렬 API 호출 적용 (멀티쓰레딩)
    results = get_scores_in_parallel(partition_list, api_key, model, max_threads=10)

    return iter(results)  # Spark RDD는 이터레이터 반환 필요


def get_score_from_gpt(car_model:str, accident:str, title:str, content: str, api_key: str, model: str = "gpt-4o-mini") -> int:
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

                각 행에는 제목(title), 본문(content)이 포함되어 있어.
                이 모든 요소를 종합하여 **해당 차량과 사고 유형에 대한 감성 점수**를 100(매우 부정적)에서 0(매우 긍정적)까지의 범위로 숫자로 평가해줘.
                50에 가까울수록 중립적이며, 100에 가까울수록 부정적, 0에 가까울수록 긍정적인거야.
                반환은 반드시 0~100사이의 정수 숫자만 반환하고 아무런 말도 하지마.
                아래는 분석할 데이터야:

                ---
                **차량 모델:** {car_model}  
                **사고 유형:** {accident}  
                **제목:** {title}  
                **본문:** {content}
                
                ---
                """
            }
        ],
        "temperature": 0.2
    }
    try:
        req = urllib.request.Request(
            url, 
            data=json.dumps(payload).encode('utf-8'),
            headers=headers,
            method='POST'
        )
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())

        content = data["choices"][0]["message"]["content"].strip()
        score = int(content)
        print(score)
        return score
    except Exception as e:
        print(f"Error while calling API: {e}. score set to 0.")
        return 0

def get_scores_in_parallel(rows, api_key, model, max_threads=10):
    """여러 행을 병렬로 API 요청하여 감성 점수를 가져오는 함수"""
    results = []

    # 🚀 ThreadPoolExecutor 사용 (최대 max_threads개의 API 요청을 동시에 실행)
    with ThreadPoolExecutor(max_threads) as executor:
        future_to_row = {
            executor.submit(get_score_from_gpt, row["car_model"], row["accident"], row["title"], row["content"], api_key, model): row
            for row in rows
        }

        # 완료된 요청부터 순차적으로 결과 처리
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            row_dict = row.asDict().copy()  # 🔥 Row 객체를 dict로 변환 후 수정 가능
            try:
                score = future.result()
                row_dict["comm_score"] = score
            except Exception as e:
                print(f"Error processing row: {e}")
                row_dict["comm_score"] = 0  # 실패 시 기본값

            results.append(Row(**row_dict))  # 🔥 dict를 다시 Row 객체로 변환하여 추가

    return results  # 모든 행의 감성 점수 포함된 리스트 반환

if __name__ == "__main__":

    # 로컬에서 사용 시 주석 해제
    # data_source = conf.S3_COMMUNITY_DATA
    # output_uri = conf.S3_COMMUNITY_OUTPUT
    # batch_period = conf.S3_COMMUNITY_BATCH_PERIOD
    # transform(data_source, output_uri, batch_period)

    # EMR에서 실행할 때 주석 해제
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_source", help="s3 data uri")
    parser.add_argument("--output_uri", help="s3 output uri")
    parser.add_argument("--batch_period", help="batch period")
    parser.add_argument("--community_accident_keyword", help="community accident keyword")
    parser.add_argument("--gpt", help="gpt information")
    parser.add_argument("--issue_list", help="issue list")
    args = parser.parse_args()
    
    transform(args.data_source, args.output_uri, args.batch_period, args.community_accident_keyword, args.gpt, args.issue_list)
