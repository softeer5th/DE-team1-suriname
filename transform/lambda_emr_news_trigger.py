import boto3
import json
import os

BUCKET_NAME = os.getenv("BUCKET_NAME")  # S3 버킷
MARKER_PATH = os.getenv("MARKER_PATH")  # JSON 저장 경로
DATA_SOURCE_PATH = os.getenv("DATA_SOURCE_PATH")
OUTPUT_PATH = os.getenv("OUTPUT_PATH")
LOG_PATH = os.getenv("LOG_PATH")
LIB_PATH = os.getenv("LIB_PATH")
REQUIRED_FILES = json.loads(os.getenv('REQUIRED_FILES'))
EMR_APPLICATION_ID = os.getenv("EMR_APPLICATION_ID")  # EMR Serverless 애플리케이션 ID
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN")  # 실행 역할 ARN
ENTRY_POINT = os.getenv("ENTRY_POINT")  # 실행할 Spark 스크립트 경로

s3_client = boto3.client("s3")
emr_client = boto3.client("emr-serverless")

def check_files_exist()->bool:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=MARKER_PATH)
    existing_files = {obj["Key"].split("/")[-1] for obj in response.get("Contents", [])}

    missing_files = [file for file in REQUIRED_FILES if file not in existing_files]

    if missing_files:
        print(f"Missing files: {missing_files}")
        return False
    else:
        print("All required files are present.")
        return True

def delete_files()->None:
    objects_to_delete = [{"Key": f"{MARKER_PATH}{file}"} for file in REQUIRED_FILES]
    s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": objects_to_delete})
    print(f"Deleted files: {REQUIRED_FILES}")

def trigger_emr_serverless()->None:
    batch_period = get_batch_period()
    print(batch_period)
    response = emr_client.start_job_run(
        applicationId=EMR_APPLICATION_ID,
        executionRoleArn=EMR_JOB_ROLE_ARN,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": ENTRY_POINT,
                "entryPointArguments": [
                    "--data_source", f"s3://{BUCKET_NAME}/{DATA_SOURCE_PATH}",
                    "--output_uri", f"s3://{BUCKET_NAME}/{OUTPUT_PATH}",
                    "--batch_period", batch_period
                ],
                "sparkSubmitParameters": f"--conf spark.executor.memory=4G --conf spark.executor.cores=2 --py-files s3://{BUCKET_NAME}/{LIB_PATH}psycopg2.zip"
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{BUCKET_NAME}/{LOG_PATH}"
                }
            }
        }
    )
    print(f"EMR Serverless job started: {response['jobRunId']}")

def get_batch_period()->str:
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{MARKER_PATH}{REQUIRED_FILES[0]}")
        json_data = json.loads(response["Body"].read())
        start_time = json_data.get("start_time")
        end_time = json_data.get("end_time")
        return f"{start_time}_{end_time}"
    except Exception as e:
        print(f"Exception getting batch_period: {e}")
        return ""

def lambda_handler(event, context):
    print("Lambda function triggered by S3 event.")

    if check_files_exist():
        trigger_emr_serverless()
        delete_files()
        return {
            "statusCode": 200,
            "body": json.dumps("EMR Serverless job triggered.")
        }

    return {
        "statusCode": 200,
        "body": json.dumps("Waiting for completion.")
    }