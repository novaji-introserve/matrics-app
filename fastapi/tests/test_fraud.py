from fastapi.testclient import TestClient

from main import app
from repo.fraud.predict import FraudPredictionResponse


client = TestClient(app)


def test_predict_fraud_high_risk(monkeypatch):
    def fake_score_fraud(_payload):
        return FraudPredictionResponse(
            fraud_probability=0.92,
            fraud_score=92,
            fraud_prediction=True,
            risk_level="high",
            model_version="test-model",
        )

    monkeypatch.setattr("router.fraud.routes.score_fraud", fake_score_fraud)
    response = client.post(
        "/fraud/predict",
        json={
            "amount": 12500,
            "transaction_date": "2024-03-27",
            "currency": "NGN",
            "account_id": 10668651,
            "terminal_id": 317,
            "cust_risk_rating": 3,
            "cust_risk_score": 9,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["risk_level"] == "high"
    assert body["fraud_score"] == 92
    assert body["fraud_prediction"] is True
    assert body["model_version"] == "test-model"


def test_predict_fraud_low_risk(monkeypatch):
    def fake_score_fraud(_payload):
        return FraudPredictionResponse(
            fraud_probability=0.12,
            fraud_score=12,
            fraud_prediction=False,
            risk_level="low",
            model_version="test-model",
        )

    monkeypatch.setattr("router.fraud.routes.score_fraud", fake_score_fraud)
    response = client.post(
        "/fraud/predict",
        json={
            "amount": 49.99,
            "transaction_date": "2024-03-27",
            "currency": "NGN",
            "account_id": 10668651,
            "terminal_id": 317,
            "cust_risk_rating": 1,
            "cust_risk_score": 2,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["risk_level"] == "low"
    assert body["fraud_score"] == 12
    assert body["fraud_prediction"] is False


def test_predict_fraud_rejects_invalid_input():
    response = client.post(
        "/fraud/predict",
        json={
            "amount": 100.0,
            "transaction_date": "2024-03-27",
            "currency": "NGN",
            "account_id": 10668651,
            "terminal_id": 317,
            "cust_risk_rating": 4,
            "cust_risk_score": 11,
        },
    )

    assert response.status_code == 422
