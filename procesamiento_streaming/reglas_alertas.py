import requests
import json

KIBANA_URL = "http://localhost:5602"

# Payload para crear la regla de caída de precio
regla_payload = {
    "params": {
        "index": ["cripto-*"],
        "timeField": "@timestamp",
        "timeWindowSize": 15,
        "timeWindowUnit": "m",
        "threshold": [0],
        "thresholdComparator": ">",
        "esQuery": "{\n  \"bool\": {\n    \"must\": [\n      {\n        \"match\": {\n          \"simbolo\": \"SOLUSDT\"\n        }\n      },\n      {\n        \"range\": {\n          \"precio\": {\n            \"lt\": 150\n          }\n        }\n      }\n    ]\n  }\n}"
    },
    "consumer": "alerts",
    "rule_type_id": "metrics.alert.threshold",
    "schedule": {"interval": "1m"},
    "actions": [],
    "name": "Alerta - Caída de Precio Cripto"
}

def crear_alerta():
    headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
    response = requests.post(
        f"{KIBANA_URL}/api/alerting/rule/alerta-precio-cripto", 
        headers=headers, 
        data=json.dumps(regla_payload)
    )
    print("Respuesta Kibana:", response.status_code, response.text)

if __name__ == "__main__":
    crear_alerta()