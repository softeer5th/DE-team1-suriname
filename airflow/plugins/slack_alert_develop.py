import json
import urllib3
from airflow.models import Variable

# Slack Webhook URL
http = urllib3.PoolManager()

def slack_failure_alert(context):
    
    slack_webhook = Variable.get("DEV_WEBHOOK_URL")
    
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    slack_data = {
        "text": f"🚨 {dag_id} 파이프라인에서 오류가 발생했어요. \n\u200b",
        "attachments":
            [{
                "color": "#FF0000",  
                "fields":
                    [
                        {
                            "title": "이슈가 발생한 Task",
                            "value": task_id,
                            "short": False
                        },
                        {
                            "title": "발생 시간",
                            "value": execution_date,
                            "short": False
                        },
                        {
                            "title": "로그 URL",
                            "value": log_url,
                            "short": False
                        },
                    ]
            }]
    }

    send_message(slack_webhook, slack_data)

    

def send_message(webhook_url: str, payload):
    response = http.request(
        'POST',
        webhook_url,
        body=json.dumps(payload),
        headers={
            'Content-Type': 'application/json'
        }
    )
    if response.status != 200:
        print("failed")
        return {
            "statusCode": 400,
            "body": json.dumps(
                f"Request to Slack returned an error {response.status}, the response is:\n{response.data}"),
            "msg": payload["text"]
        }
    else:
        print("success")
        return {
            "statusCode": 200,
            "body": json.dumps("Message was successfuly sent to Slack."),
            "msg": payload["text"]
        }
