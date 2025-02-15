import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import boto3
import json
import time

rds_host = os.getenv("RDS_HOST")
rds_db = os.getenv("RDS_DB")
rds_user = os.getenv("RDS_USER")
rds_password = os.getenv("RDS_PASSWORD")

redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_workgroup = os.getenv("REDSHIFT_WORKGROUP")

redshift_client = boto3.client("redshift-data")

db_url = f"postgresql://{rds_user}:{rds_password}@{rds_host}/{rds_db}"
rds_engine = create_engine(db_url)

def get_df_from_rds(schema_name, table_name):
    try:
        query = f"SELECT * FROM {schema_name}.{table_name};"
        with rds_engine.connect() as connection:
            df = pd.read_sql(query, connection)
        print("Fetched columns:", df.columns.tolist())
        return df
    except Exception as e:
        print(f"Error fetching data from RDS: {e}")
        return None

def execute_redshift_query(sql_query):
    try:
        print(f"Executing query: {sql_query}")  # 실행 쿼리 출력
        response = redshift_client.execute_statement(
            WorkgroupName=redshift_workgroup,
            Database=redshift_db,
            Sql=sql_query
        )
        query_id = response['Id']
        print(f"Query submitted with ID: {query_id}")

        # 실행 상태 확인
        while True:
            status_response = redshift_client.describe_statement(Id=query_id)
            status = status_response['Status']
            if status in ["FINISHED", "FAILED", "ABORTED"]:
                break
            print(f"Query {query_id} is still running...")  # 실행 상태 확인
            time.sleep(1)  # 1초 대기 후 다시 확인
        
        if status == "FINISHED":
            print(f"Query {query_id} executed successfully.")
            return query_id
        else:
            error_message = status_response.get("Error", "No error message provided")
            print(f"Query {query_id} failed with status: {status}. Error: {error_message}")
            return None

    except Exception as e:
        print(f"Error executing Redshift query: {e}")
        return None

def create_redshift_table_and_insert(df, table_name):
    try:
        create_query = f"""
        CREATE TABLE IF NOT EXISTS test.{table_name} (
            car_model VARCHAR(255),
            accident VARCHAR(255),
            accumulated_count INT4,
            start_batch_time TIMESTAMP,
            last_batch_time TIMESTAMP,
            contents SUPER,
            is_issue BOOL,
            is_alert BOOL,
            issue_score INT4
        );
        """.strip()
        
        print(f"Generated CREATE TABLE query: {create_query}")
        query_id = execute_redshift_query(create_query)
        if query_id:
            print(f"Table {table_name} creation query submitted with ID: {query_id}")
        else:
            print(f"Failed to submit table creation query for {table_name}")
            return
        
        for _, row in df.iterrows():
            # 각 데이터 타입에 맞게 값을 포맷팅
            formatted_values = []
            for val, col_name in zip(row.values, df.columns):
                if val is None:
                    formatted_values.append('NULL')
                elif col_name in ['is_issue', 'is_alert']:
                    formatted_values.append(str(val).lower())  # true/false로 변환
                elif col_name == 'contents':
                    formatted_values.append(f"'{json.dumps(val, ensure_ascii=False)}'")  # 한글 인코딩 유지
                elif col_name in ['accumulated_count', 'issue_score']:
                    formatted_values.append(str(val))  # 숫자는 따옴표 없이
                else:
                    formatted_values.append(f"'{str(val)}'")  # 나머지는 문자열로

            values = ", ".join(formatted_values)
            insert_query = f"""
            INSERT INTO test.{table_name}
            SELECT {values}
            WHERE NOT EXISTS (
                SELECT 1 
                FROM test.{table_name}
                WHERE car_model = {formatted_values[0]}
                AND accident = {formatted_values[1]}
            );
            """
            query_id = execute_redshift_query(insert_query)
            if query_id:
                print(f"Inserted row into {table_name} with query ID: {query_id}")
            else:
                print(f"Failed to insert row into {table_name}")
                return
    except Exception as e:
        print(f"Error in Redshift operation: {e}")

def lambda_handler(event, context):
    try:
        start_time = datetime.strptime(event.get('start_time_str'), "%Y-%m-%dT%H:%M")
        batch_start_time = start_time.strftime('%Y%m%d_%H%M%S')
        table_name = f"t_{batch_start_time}_main"  # 숫자로 시작하지 않도록 "t_" 추가

        df = get_df_from_rds('test', 'accumulated_table')
        if df is None:
            return {
                "statusCode": 500,
                "body": json.dumps("Failed to fetch data from RDS.")
            }
        print(f"Fetched {df.shape[0]} rows from RDS.")

        if df.empty:
            return {
                "statusCode": 404,
                "body": json.dumps("No data found in the table.")
            }
        
        create_redshift_table_and_insert(df, table_name)

        return {
            "statusCode": 200,
            "body": json.dumps(f"Table {table_name} created and data inserted successfully.")
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("Internal Server Error")
        }
