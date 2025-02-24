import boto3
import time
import logging
from conf import *
import base64
import json
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_to_s3(local_script_path:str, s3_script_path:str):
    """S3에 파일 업로드"""
    s3_client = boto3.client('s3', region_name="ap-northeast-2")
    try:
        s3_client.upload_file(local_script_path, BUCKET_NAME, s3_script_path)
        logger.info("emr script upload succeed.")
        return True
    except Exception as e:
        logger.info(e)
        logger.info("emr script upload failed.")
        return False

def encode_parameters(issue_list, accident_keyword):
    """conf.py에서 파라미터를 가져와서 base64로 인코딩"""
    # 이슈 리스트 인코딩
    issue_list_encoded = base64.b64encode(
        json.dumps(ISSUE_LIST, ensure_ascii=False).encode('utf-8')
    ).decode('utf-8')

    # 사고 키워드 인코딩
    accident_keywords_encoded = base64.b64encode(
        json.dumps(COMMUNITY_ACCIDENT_KEYWORD, ensure_ascii=False).encode('utf-8')
    ).decode('utf-8')


    return issue_list_encoded, accident_keywords_encoded

def submit_job(s3_script_path:str,
               emr_job_name:str,
               s3_data_source_uri:str,
               s3_output_uri:str,
               batch_period:str,
               accident_keyword,
               issue_list):
    """EMR Serverless에 Spark 작업 제출"""
    emr_client = boto3.client('emr-serverless', region_name="ap-northeast-2")

    s3_script_location = f's3://{BUCKET_NAME}/{s3_script_path}'
    # 파라미터 인코딩
    issue_list_encoded, accident_keywords_encoded = encode_parameters(issue_list, accident_keyword)


    try:
        response = emr_client.start_job_run(
            applicationId=EMR_APPLICATION_ID,
            clientToken=str(time.time()),
            name=emr_job_name,
            executionRoleArn=EMR_EXECUTION_ROLE_ARN,
            jobDriver={
                'sparkSubmit': {
                    'entryPoint': s3_script_location,
                    'sparkSubmitParameters': '--conf spark.executor.cores=4 --conf spark.executor.memory=8g',
                    'entryPointArguments': [
                        '--data_source', s3_data_source_uri,
                        '--output_uri', s3_output_uri,
                        "--batch_period", batch_period,  # test
                        "--community_accident_keyword", accident_keywords_encoded,
                        "--gpt", json.dumps(GPT, ensure_ascii=False),
                        "--issue_list", issue_list_encoded
                    ]
                }
            }
        )

        job_run_id = response['jobRunId']
        logger.info(f"작업 제출 완료. Job Run ID: {job_run_id}")
        return job_run_id

    except Exception as e:
        logger.error(f"작업 제출 실패: {str(e)}")
        return None


def main(
        local_script_path:str,
        s3_script_path:str,
        emr_job_name:str,
        s3_data_source_uri:str,
        s3_output_uri:str,
        batch_period:str,
        accident_keyword:list,
        issue_list:list
):
    # 1. S3에 스크립트 업로드
    if upload_to_s3(local_script_path, s3_script_path):
        # 2. EMR Serverless 작업 제출
        job_run_id = submit_job(
            s3_script_path, emr_job_name, s3_data_source_uri, s3_output_uri, batch_period, accident_keyword, issue_list)
        if job_run_id:
            logger.info("작업이 성공적으로 제출되었습니다.")


if __name__ == "__main__":
    main(
        local_script_path=EMR_COMM_SCRIPT_LOCAL,
        s3_script_path=EMR_COMM_SCRIPT_S3,
        emr_job_name=EMR_COMM_JOB_NAME,
        s3_data_source_uri=S3_COMMUNITY_DATA,
        s3_output_uri=S3_COMMUNITY_OUTPUT,
        batch_period=S3_COMMUNITY_BATCH_PERIOD,
        accident_keyword=COMMUNITY_ACCIDENT_KEYWORD,
        issue_list=ISSUE_LIST
    )
