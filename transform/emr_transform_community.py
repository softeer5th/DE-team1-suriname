from functools import partial
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
from datetime import datetime
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
err_cnt = 0
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

        df = df.withColumn(
            "content",
            F.regexp_replace(
                F.col("content"),
                "[^가-힣a-zA-Z0-9\\s]",
                ""
            )
        )

        df = df.withColumn(
            "title",
            F.regexp_replace(
                F.col("title"),
                "[^가-힣a-zA-Z0-9\\s]",
                ""
            )
        )

        
        # issue_list에 해당하는 사고 유형만 추출
        selected_keywords = {issue['accident']: community_accident_keyword[issue['accident']] for issue in issue_list if issue['accident'] in community_accident_keyword}
        logger.info(f"selected_keywords: {selected_keywords}")

        # `car_model` 매칭을 위한 딕셔너리 생성 (accident → car_model 매핑)
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

        # df_exploded.show()
        # `accident_to_car_model`을 Spark DataFrame으로 변환
        mapping_data = [(accident, car_model) for accident, car_models in accident_to_car_model.items() for car_model in car_models]
        mapping_df = spark.createDataFrame([Row(accident=a, car_model=c) for a, c in mapping_data])

        # `accident` 기준으로 `join`
        df_exploded = df_exploded.join(mapping_df, on="accident", how="left")
        df_exploded.show()
        # `car_model`이 issue_list에 있는 자동차 모델만 필터링
        # car_models = [issue["car_model"] for issue in issue_list]
        # df_filtered = df_exploded.filter(F.col("car_model").isin(car_models))
        

        score_schema = df_exploded.schema.add('comm_score', IntegerType(), True)
        gpt = json.loads(gpt)

        start_date, end_date = batch_period.split("_")
        # start_date = datetime.strptime(start_date, "%Y-%m-%d-%H-%M-%S")
        # end_date = datetime.strptime(end_date, "%Y-%m-%d-%H-%M-%S")

        print(df_exploded.count())
        batch_post = df_exploded.filter(
            F.col("post_time").between(F.to_timestamp(F.lit(start_date), "yyyy-MM-dd-HH-mm-ss"), F.to_timestamp(F.lit(end_date), "yyyy-MM-dd-HH-mm-ss"))
        )
        print(batch_post.count())
        # filtered_df = df_exploded.filter(
        #     (F.col("post_time") >= F.to_timestamp(F.lit(start_date), "yyyy-MM-dd-HH-mm-ss")) &
        #     (F.col("post_time") <= F.to_timestamp(F.lit(end_date), "yyyy-MM-dd-HH-mm-ss"))
        # )

        batch_post = batch_post.limit(300)
        score_rdd = batch_post.rdd.mapPartitions(partial(score_rdd_generator, param = gpt))
        scored_df = score_rdd.toDF(score_schema).cache()
        scored_df.show()
        scored_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/' + "debug/")

        # 평균 점수, 개수를 카테고리별로 계산
        avg_scores_df = scored_df.groupBy("car_model", "accident").agg(
            F.avg("comm_score").alias("avg_comm_score"),
            F.count("*").alias("count"),
            F.sum(F.when(F.col("comm_score") <= 45, 1).otherwise(0)).alias("comm_positive_count"),
            F.sum(F.when(F.col("comm_score") >= 55, 1).otherwise(0)).alias("comm_negative_count")
        )
        avg_scores_df.show()

        window_spec = Window.partitionBy("car_model", "accident").orderBy(F.desc("view_count"))

        top_post_df = df_exploded.withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") <= 5) \
            .drop("rank")

        # final_df = scored_df.groupBy("car_model", "accident").agg(
        #     F.count("*").alias("count"),
        #     F.to_json(
        #         F.collect_list(
        #             F.struct("content", "link")
        #         )
        #     ).alias("comm"),
        #     F.avg("comm_score").alias("avg_comm_score")
        # )

        # 조회수 상위 다섯개 df
        grouped_df = top_post_df.groupBy("car_model", "accident").agg(
            F.to_json(
                F.collect_list(
                    F.struct("title", "content", "link", "view_count", "like_count")
                )
            ).alias("top_comm")
        )

        # 사건별 그룹화 된 것에 평균 점수, 게시글 개수 추가
        final_df = avg_scores_df.join(
            grouped_df,
            on=["car_model", "accident"],
            how="left"
        )

        final_df.show()

        final_df.coalesce(1).write.mode('overwrite').parquet(output_uri + batch_period + '/')
        logger.info(err_cnt)
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

    partition_list = list(partition)  # partition을 리스트로 변환 (멀티쓰레딩 적용 가능)

    if not partition_list:
        return iter([])  # 빈 partition 처리

    # 병렬 API 호출 적용 (멀티쓰레딩)
    results = get_scores_in_parallel(partition_list, api_key, model, max_threads=5)

    return iter(results)  # Spark RDD는 이터레이터 반환 필요


def get_score_from_gpt(car_model:str, accident:str, title:str, content: str, api_key: str, model: str = "gpt-4o-mini") -> int:
    global err_cnt
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content" : f"""
                    너는 현대자동차 이슈관리팀의 AI야.
                    지금 {car_model}, {accident} 사고가 발생했어.
                    너는 사건의 일어난 시간대의 자동차 커뮤니티의 글을 확인하고 있어. 커뮤니티에서는 사고 이야기를 하고 있기도 하고, 아닌글들도 있어.
                    주어진 커뮤니티 글에 대한 정보는 제목(title)과 본문(content)야.
                    제목과 본문을 보고 글쓴이가 판단하는 사건에 대한 책임 소재를 현대자동차 입장에서 판단해야해.
                    일단 글을 봤을 때 이슈에 대한 책임을 어디에 물고 있는지가 중요하겠지?
                    예를 들어서 car_model = '제네시스', accident = '급발진' 이라고 해보자.
                    {title}과 {content}를 종합해서 봤을 때, 

                    '급발진이 맞는 것 같다.' '현대차 잘못이다.' '제네시스 차량에 문제가 있네.' '급발진 아님?'
                    이런 느낌으로 {accident}에 대해 차량이나 차를 만든 회사에 책임을 묻는 듯한 뉘앙스거나 차량이나 회사에 대한 의구심, 사건에 
                    대한 의심을 제기하거나, 현대자동차에 대해 욕하는 듯한 뉘앙스라면 현대자동차 입장에서 회사에 책임을 물고 있는 글이겠지.

                    '이게 무슨 급발진이야.' '운전자 잘못이네.'
                    이런 느낌으로 {accident}에 대해 차량이나 차를 만든 회사의 책임보다는 운전자같은 다른 곳에 책임이 있다거나,
                    운전자를 욕하는 듯한 뉘앙스라고 느껴지면 현대자동차 입장에서 크게 의식 안해도 되는 운전자에 책임을 물고 있는 글이겠지.

                    그리고 사고와 무관한 얘기를 하거나 사고와 관련된 얘기를 해도 글을 봤을 때 이 사람이 주장하는 책임 소재를 
                    따지기 어려우면 그것은 중립적인 글이라고 판단할 수 있어. 꼭 {accident} 단어를 언급하지 않아야
                    사고와 관련 없는 얘기를 하는건 아니고, {accident} 단어를 사용해도 이 사고와 관련된 얘기가 
                    아니라면 {accident}에 대한 얘기가 아니므로 중립으로 판단할 수 있어야 해.


                    반환을 "과실 매우 높음", "과실 높음", "중립", "과실 낮음", "과실 매우 낮음" 이렇게 5단계로 나누어서 먼저 해줘.
                    그 다음 분류 안에서 점수까지 같이 산정을 해줘. 
                    100점에 가까울 수록 현대차에 책임을 물고 있는 글이고, 
                    0점에 가까울 수록 운전자에 책임을 물고 있는 글이겠지.
                    긍정적인 글이지.
                    "과실 매우 높음" 이라고 판단했을 경우 그 정도를 76점~100점 사이의 지표로 내주고,
                    "과실 높음" 이라고 판단했을 경우 그 정도를 51점~75점 사이의 지표로 내주고,
                    "중립" 이라고 판단했거나 아예 관련 없는 글이라고 생각하면 50점 지표로 내주고,
                    "과실 낮음" 이라고 판단했을 경우 그 정도를 25점~49점 사이의 지표로 내주고,
                    "과실 매우 낮음" 이라고 판단했을 경우 그 정도를 0점~24점 사이의 지표로 내줘.

                    Input은 다음과 같이 줄게.
                    ---
                    **차량 모델:** {car_model}  
                    **사고 유형:** {accident}  
                    **제목:** {title}  
                    **본문:** {content}

                    ---

                    반환은
                    JSON 형태로 해줘.
                    ex)
                    {{"sentiment":"과실 낮음", "score":30, "reason":"판단한 이유 간략히"}}
                """,
            },{
                "role": "user",
                "content": 
                    """
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

        req = urllib.request.Request(
            url, 
            data=json.dumps(payload).encode('utf-8'),
            headers=headers,
            method='POST'
        )
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())

        content = data["choices"][0]["message"]["content"].strip()

        if content.startswith("```json"):
            content = content.split('\n', 1)[1]
            if content.endswith("```"):
                content = content.rsplit('\n', 1)[0]

        content_data = json.loads(content)
        score = int(content_data["score"])
        return score
    
    except Exception as e:
        print(f"Error while calling API: {e}. score set to 0.")
        # err_cnt += 1
        logger.info(f"gpt {e}")
        return -1000

def get_scores_in_parallel(rows, api_key, model, max_threads=10):
    """여러 행을 병렬로 API 요청하여 감성 점수를 가져오는 함수"""
    results = []

    # ThreadPoolExecutor 사용 (최대 max_threads개의 API 요청을 동시에 실행)
    with ThreadPoolExecutor(max_threads) as executor:
        future_to_row = {
            executor.submit(get_score_from_gpt, row["car_model"], row["accident"], row["title"], row["content"], api_key, model): row
            for row in rows
        }

        # 완료된 요청부터 순차적으로 결과 처리
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            row_dict = row.asDict().copy()  # Row 객체를 dict로 변환 후 수정 가능
            try:
                score = future.result()
                row_dict["comm_score"] = score
            except Exception as e:
                print(f"Error processing row: {e}")
                logger.info(f"{e}")
                row_dict["comm_score"] = -100  # 실패 시 기본값

            results.append(Row(**row_dict))  # dict를 다시 Row 객체로 변환하여 추가

    return results  # 모든 행의 감성 점수 포함된 리스트 반환

if __name__ == "__main__":

    # 로컬에서 사용 시 주석 해제
    # data_source = conf.S3_COMMUNITY_DATA
    # output_uri = conf.S3_COMMUNITY_OUTPUT
    # batch_period = conf.S3_COMMUNITY_BATCH_PERIOD
    # accident_keyword = conf.ACCIDENT_KEYWORD
    # gpt = conf.GPT
    # issue_list = conf.ISSUE_LIST
    # transform(data_source, output_uri, batch_period, accident_keyword, gpt, issue_list)

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
