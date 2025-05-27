import asyncio
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta
import uvicorn
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

ALERT_LOG = 'alerts.log'

app = FastAPI()
alerts: List[dict] = []
alert_count = 0
last_summary_time = datetime.utcnow()

metrics = {
    'alerts_raised': 0,
    'max_processing_latency': 0.0  # in seconds
}

# Prometheus metrics
ALERTS_RAISED = Counter('alerts_raised', 'Total number of alerts raised')
ALERT_PROCESSING_LATENCY = Histogram('alert_processing_latency_seconds', 'Alert processing latency in seconds')

class Alert(BaseModel):
    transaction_id: str
    amount: float
    timestamp: str
    location: str
    card_id: str
    merchant_id: str
    risk: int
    fraud: bool

@app.post('/alerts')
async def receive_alert(alert: Alert, request: Request):
    global alert_count
    with ALERT_PROCESSING_LATENCY.time():
        alerts.append(alert.dict())
        alert_count += 1
        with open(ALERT_LOG, 'a') as f:
            f.write(f"{datetime.utcnow().isoformat()} ALERT: {alert.dict()}\n")
        ALERTS_RAISED.inc()
    return {"status": "received"}

@app.get('/metrics')
def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

async def summary_task():
    global alert_count, last_summary_time
    while True:
        await asyncio.sleep(60)
        now = datetime.utcnow()
        summary = f"{now.isoformat()} - Alerts in last 60s: {alert_count}\n"
        with open(ALERT_LOG, 'a') as f:
            f.write(summary)
        print(summary.strip())
        alert_count = 0
        last_summary_time = now

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(summary_task())

if __name__ == "__main__":
    uvicorn.run("fd_kafka.alert_dispatcher:app", host="0.0.0.0", port=8000, reload=True)
