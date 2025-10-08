import requests
from airflow.models import Variable

# Haven't added slack_webhook_url variable in airflow as I was not able to create a new app and alow incoming webhooks. 
# This is the reason the DAGs will fail if run at the moment.

def slack_notify(message: str, channel: str = None):
    webhook_url = Variable.get("slack_webhook_url")
    payload = {
        "text": message,
    }
    if channel:
        payload["channel"] = channel
    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()
    print(f"✅ Slack notification sent: {message}")
