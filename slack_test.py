import os
import json
import urllib3

# Slack Webhook URL
webhook_url = os.getenv('WEBHOOK_URL')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    try:
        # subject = event['Records'][0]['Sns']['Subject']
        # message = event['Records'][0]['Sns']['Message']

        # Slack 메시지 구성
        # text = f"*{subject}*\n\n{message}"
        text = "Hello, Slack!"
        print(f"[INFO] message:\n{text}")
        slack_message = {
            "channel": "C08DSB1BQ1G",
            "username": "이슈 알리미",
            "text": text,
            "icon_emoji": ":rotating_light:"
        }

        # Slack으로 메시지 전송
        response = http.request(
            'POST',
            webhook_url,
            body=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'}
        )

        if response.status != 200:
            return {
                "statusCode": 200,
                "body": json.dumps(f"Request to Slack returned an error {response.status}, the response is:\n{response.data}"),
                "msg": text
            }
        else:
            return {
                "statusCode": 200,
                "body": json.dumps("Message was successfuly sent to Slack."),
                "msg": text
            }

    except Exception as e:
        print(f"[ERROR] Failed to process SNS message: {str(e)}")
        raise e
