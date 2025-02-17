from datetime import datetime
import json
import boto3

NEWS_SOURCES = ["kbs", "sbs", "yna", "ytn"]
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

def lambda_handler(event, context):
    try:
        start_time = event.get('start_time_str')
        end_time = event.get('end_time_str')

        if not start_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - Start_time.')}
        if not end_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - End_time.')}

        for source in NEWS_SOURCES:
            request = {
                "start_time_str": start_time,
                "end_time_str": end_time,
                "source" : source
            }
            trigger_news_crawler(request)


        return {'statusCode': 200, 'body': json.dumps("All news crawlers triggered asynchronously")}
    except ValueError as valueError:
        return {'statusCode': 400, 'body': json.dumps(str(valueError))}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error: Unknown error occured. {str(e)}")}

def trigger_news_crawler(news_event):
    response = lambda_client.invoke(
        FunctionName='news_extract_function',  
        InvocationType='Event',             
        Payload=json.dumps(news_event)
    )

    print(news_event["source"]," Triggered with status:", response.get("StatusCode"))