import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

def send_alert(alert_type: str, message: str, severity: str = "WARNING"):
    alert_payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "severity": severity,
        "alert_type": alert_type,
        "message": message,
        "service": "qcommerce-telemetry-engine"
    }
    
    # In production, replace with: requests.post(WEBHOOK_URL, json=alert_payload)
    logging.info(f"📢 [ALERT DISPATCH] {severity} - {alert_type}: {message}")
    
    with open("pipeline_alerts.log", "a") as f:
        f.write(json.dumps(alert_payload) + "\n")

if __name__ == "__main__":
    send_alert(
        alert_type="DATA_QUALITY_ANOMALY",
        message="Simulated alert: 5% DLQ anomaly spike detected in recent ingestion window.",
        severity="HIGH"
    )