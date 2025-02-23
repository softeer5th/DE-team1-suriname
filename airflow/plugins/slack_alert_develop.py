import json
import urllib3

# Slack Webhook URL
http = urllib3.PoolManager()

def send_message(webhook_url: str, payload):
        slack_data = {
            "text": f"🚨 {payload["dag_id"]} 파이프라인에서 오류가 발생했어요. 🚨",
            "attachments": [{
                "color": "#FF0000",  
                "fields": [
                    {
                        "title": "이슈가 발생한 Task",
                        "value": payload["task_id"],
                        "short": False
                    },
                    {
                        "title": "발생 시간",
                        "value": payload["execution_date"], 
                        "short": False
                    },
                    {
                        "title": "로그 URL",
                        "value": payload["log_url"],
                        "short": False
                    },
                ]
            }]
        }
        print("메시지 전송 호출!!!!!!!!!!")
        response = http.request(
            'POST',
            webhook_url,
            body=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status != 200:
            print("failed")
        else:
            print("success")
        

def lambda_handler(event, context):
    webhook_url = event['webhook_url']
    payload = event['payload']

    try:
        send_message(webhook_url, payload)

    except Exception as e:
        print(f"[ERROR] Failed to process SNS message: {str(e)}")
        raise e