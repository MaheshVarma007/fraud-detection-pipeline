import pytest
import httpx
from fastapi import status
from fd_kafka.alert_dispatcher import app

@pytest.fixture
async def test_client():
    async with httpx.AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

@pytest.mark.asyncio
async def test_metrics_initial(test_client):
    response = await test_client.get("/metrics")
    assert response.status_code == 200
    assert b"alerts_raised" in response.content
    assert b"alert_processing_latency_seconds" in response.content

@pytest.mark.asyncio
async def test_alert_and_metrics(test_client):
    alert = {
        "transaction_id": "test123",
        "amount": 25000.0,
        "timestamp": "2025-05-28T12:00:00Z",
        "location": "TestCity",
        "card_id": "CARDTEST",
        "merchant_id": "MERCHANTTEST",
        "risk": 90,
        "fraud": True
    }
    # Post alert
    resp = await test_client.post("/alerts", json=alert)
    assert resp.status_code == status.HTTP_200_OK
    # Check metrics updated
    metrics_resp = await test_client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert b"alerts_raised" in metrics_resp.content
    assert b"alert_processing_latency_seconds" in metrics_resp.content
