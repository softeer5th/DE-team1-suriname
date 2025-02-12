import boto3
import conf

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    is_done = True
    missing_site_list = []

    try:
        response = s3_client.list_objects_v2(Bucket=conf.BUCKET_NAME, Prefix=conf.MARKER_PATH)
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        raise e

    marker_files = set()
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith('.done'):
                marker_files.add(key)
    print(marker_files)

    for site in conf.TARGET_SITE:
        expected_marker = f"{conf.MARKER_PATH}{site}.done"
        if expected_marker not in marker_files:
            is_done = False
            missing_site_list.append(site)

    if is_done:
        print("모든 크롤링 Lambda 작업이 완료")
        try:
            delete_objects = [{"Key": key} for key in marker_files]  # 삭제할 파일 리스트 생성

            if delete_objects:
                s3_client.delete_objects(
                    Bucket=conf.BUCKET_NAME,
                    Delete={"Objects": delete_objects}
                )
                print(f"🗑️ 삭제된 파일 목록: {marker_files}")

        except Exception as e:
            print(f"Done 파일 삭제 중 오류 발생: {e}")
            raise e
    else:
        print(f"아직 완료되지 않은 작업: {missing_site_list}")

    return {
        "is_done": is_done,
        "missing_sites": missing_site_list
    }