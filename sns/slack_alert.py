import json
import urllib3
import psycopg2

# Slack Webhook URL
http = urllib3.PoolManager()


def build_payload(text, issue: dict) -> dict:
    # 30 -> 관심
    # 60 -> 주의
    # 100 -> 긴급
    issue_payload_param = dict()
    issue_payload_param["initial_issue_time"] = issue["start_batch_time"].isoformat()
    issue_payload_param["car_model"] = issue["car_model"]
    issue_payload_param["accident"] = issue["accident"]
    issue_payload_param["dashboard_url"] = issue["dashboard_url"]
    if issue["issue_score"] <= 0.3:
        issue_payload_param["color"] = "#FFD700"
        issue_payload_param["level_value"] = "관심 🟡"
    elif issue["issue_score"] <= 0.6:
        issue_payload_param["color"] = "#FFA500"
        issue_payload_param["level_value"] = "주의 🟠"
    else:
        issue_payload_param["color"] = "#FF0000"
        issue_payload_param["level_value"] = "긴급 🚨"

    payload = \
        {
            "text": f"🚨 {issue_payload_param['car_model']} {issue_payload_param['accident']} ({issue_payload_param['level_value']})\n\u200b",
            "attachments":
                [{
                    "color": issue_payload_param["color"],  # 메시지 강조 색상 (빨간색)
                    "fields":
                        [
                            {
                                "title": "이슈 주의도",
                                "value": issue_payload_param["level_value"],
                                "short": False
                            },
                            {
                                "title": "이슈 내용",
                                "value": f"{issue_payload_param["car_model"]} {issue_payload_param["accident"]}",
                                "short": False
                            },
                            {
                                "title": "이슈 최초 발생",
                                "value": issue_payload_param["initial_issue_time"],
                                "short": False
                            },
                            {
                                "title": "대시보드 링크",
                                "value": issue_payload_param["dashboard_url"],
                                "short": False
                            }
                        ]
                }]
        }
    return payload


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


def get_alert_issue(event) -> list[dict]:
    alert_issue_list = []
    conn = psycopg2.connect(
        dbname=event["dbname"],
        user=event["user"],
        password=event["password"],
        host=event["url"],
        port=event["port"]
    )
    cursor = conn.cursor()
    get_alert_issue_query = f"""
        SELECT car_model, accident, start_batch_time, news_acc_count, comm_acc_count, issue_score, dashboard_url
        FROM accumulated_table
        WHERE is_alert = TRUE;
        """
    try:
        cursor.execute(get_alert_issue_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"db connect failed.. {e}")
        conn.rollback()

    cursor.close()
    conn.close()
    return result


def lambda_handler(event, context):
    webhook_url = event['webhook_url']
    try:
        text = ""
        alert_issue_list = get_alert_issue(event)
        for issue in alert_issue_list:
            payload = build_payload(text, issue)
            send_message(webhook_url, payload)

    except Exception as e:
        print(f"[ERROR] Failed to process SNS message: {str(e)}")
        raise e