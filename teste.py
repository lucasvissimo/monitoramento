#testando alertas slack

import requests
import json

url_post_msg = "https://hooks.slack.com/services/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"

message = "teste"
headers = {
    "Content-type": "application/json"
}
payload = {
    "text": message
}

r = requests.post(url_post_msg, headers=headers, data=json.dumps(payload))

print(r.status_code)
print(r.text)
